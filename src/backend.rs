use crate::{
    codec::{Decoder, DisplayFormat},
    model::*,
    storage,
};
use crate::{i18n::tr, tf};
use anyhow::{Context, Result, bail, ensure};
use rdkafka::{
    ClientConfig, ClientContext, Message, Offset, TopicPartitionList,
    consumer::{BaseConsumer, Consumer},
    error::{KafkaError, RDKafkaErrorCode},
    message::{BorrowedMessage, Header, Headers, OwnedHeaders},
    producer::{
        BaseProducer, BaseRecord, DeliveryResult, FutureProducer, FutureRecord, ProducerContext,
    },
};
use regex::RegexBuilder;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

pub type Cancellation = Arc<AtomicBool>;
type ScanBounds = BTreeMap<(String, i32), (i64, i64)>;
pub fn cancellation() -> Cancellation {
    Arc::new(AtomicBool::new(false))
}

// Queue at most 8192 messages / 8 MiB, then wait for acknowledgements. A single
// larger message is allowed, but cannot share the window with another message.
struct DeliveryWindow {
    producer: BaseProducer<DeliveryContext>,
    state: Arc<Mutex<DeliveryState>>,
    enqueued: usize,
    delivered: usize,
    receipts: Vec<(String, i32, i64)>,
}
#[derive(Default)]
struct DeliveryState {
    pending: usize,
    pending_bytes: usize,
    delivered: usize,
    receipts: Vec<(String, i32, i64)>,
    error: Option<String>,
}
struct DeliveryContext(Arc<Mutex<DeliveryState>>);
impl ClientContext for DeliveryContext {}
impl ProducerContext for DeliveryContext {
    type DeliveryOpaque = usize;
    fn delivery(&self, result: &DeliveryResult<'_>, bytes: usize) {
        let mut state = self.0.lock().unwrap();
        state.pending -= 1;
        state.pending_bytes -= bytes;
        match result {
            Ok(message) => {
                state.delivered += 1;
                if state.receipts.len() < 1000 {
                    state.receipts.push((
                        message.topic().into(),
                        message.partition(),
                        message.offset(),
                    ));
                }
            }
            Err((error, _)) => {
                state.error.get_or_insert_with(|| error.to_string());
            }
        }
    }
}
fn future_record(r: &ProduceRecord) -> FutureRecord<'_, [u8], [u8]> {
    let mut message = FutureRecord::<[u8], [u8]>::to(&r.topic);
    if !r.headers.is_empty() {
        let mut headers = OwnedHeaders::new();
        for h in &r.headers {
            headers = headers.insert(Header {
                key: &h.key,
                value: h.value.as_deref(),
            });
        }
        message = message.headers(headers);
    }
    if let Some(key) = &r.key {
        message = message.key(key);
    }
    if let Some(value) = &r.value {
        message = message.payload(value);
    }
    if let Some(p) = r.partition {
        message = message.partition(p);
    }
    if let Some(t) = r.timestamp {
        message = message.timestamp(t);
    }
    message
}
impl DeliveryWindow {
    fn new(config: ClientConfig) -> Result<Self> {
        let state = Arc::new(Mutex::new(DeliveryState::default()));
        let producer = config.create_with_context(DeliveryContext(state.clone()))?;
        Ok(Self {
            producer,
            state,
            enqueued: 0,
            delivered: 0,
            receipts: vec![],
        })
    }
    fn poll(&mut self, wait: Duration) -> Result<()> {
        self.producer.poll(wait);
        let state = self.state.lock().unwrap();
        self.delivered = state.delivered;
        if let Some(error) = &state.error {
            bail!(tf!(
                "Kafka nao confirmou uma mensagem: {0}. Em timeout, verifique o destino antes de repetir",
                error
            ));
        }
        Ok(())
    }
    fn enqueue(&mut self, record: &ProduceRecord) -> Result<()> {
        let bytes = record.key.as_ref().map_or(0, Vec::len)
            + record.value.as_ref().map_or(0, Vec::len)
            + record
                .headers
                .iter()
                .map(|h| h.key.len() + h.value.as_ref().map_or(0, Vec::len) + 64)
                .sum::<usize>()
            + 256;
        loop {
            let full = {
                let state = self.state.lock().unwrap();
                state.pending > 0
                    && (state.pending >= 8192
                        || state.pending_bytes.saturating_add(bytes) > 8 * 1024 * 1024)
            };
            if !full {
                break;
            }
            self.poll(Duration::from_millis(10))?;
        }
        loop {
            let future = future_record(record);
            let message = BaseRecord {
                topic: future.topic,
                partition: future.partition,
                payload: future.payload,
                key: future.key,
                timestamp: future.timestamp,
                headers: future.headers,
                delivery_opaque: bytes,
            };
            match self.producer.send(message) {
                Ok(()) => {
                    {
                        let mut state = self.state.lock().unwrap();
                        state.pending += 1;
                        state.pending_bytes += bytes;
                    }
                    self.enqueued += 1;
                    // Poll on this worker rather than allocating a future per record
                    // and running a separate thread that waits 100 ms between polls.
                    if self.enqueued.is_multiple_of(64) {
                        self.poll(Duration::ZERO)?;
                    }
                    return Ok(());
                }
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _))
                    if self.state.lock().unwrap().pending > 0 =>
                {
                    self.poll(Duration::from_millis(10))?
                }
                Err((e, _)) => {
                    return Err(e).context(tr("Falha ao enfileirar mensagem; operacao parcial"));
                }
            }
        }
    }
    fn finish(&mut self) -> Result<()> {
        // Also drain on cancellation or failure: no success before all queued
        // messages have a final delivery result, and no invisible pending sends.
        while self.state.lock().unwrap().pending > 0 {
            let _ = self.poll(Duration::from_millis(10));
        }
        let mut state = self.state.lock().unwrap();
        self.delivered = state.delivered;
        self.receipts = std::mem::take(&mut state.receipts);
        if let Some(error) = &state.error {
            bail!(tf!(
                "{0}; {1} mensagens confirmadas. Verifique o destino antes de repetir",
                error,
                self.delivered
            ));
        }
        Ok(())
    }
}
pub struct Backend {
    pub profile: ClusterProfile,
    reader: Mutex<Option<ReadSession>>,
}
struct ReadSession {
    consumer: BaseConsumer,
    config: BTreeMap<String, String>,
}
impl Backend {
    pub fn new(profile: ClusterProfile) -> Self {
        Self {
            profile,
            reader: Mutex::new(None),
        }
    }
    pub fn config(&self) -> Result<ClientConfig> {
        let p = &self.profile;
        ensure!(
            !p.bootstrap_servers.trim().is_empty(),
            tr("Informe bootstrap servers")
        );
        ensure!(
            ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]
                .contains(&p.security_protocol.as_str()),
            tr("Protocolo de seguranca invalido")
        );
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &p.bootstrap_servers)
            .set("client.id", "kafunekafka")
            .set("security.protocol", &p.security_protocol)
            .set("socket.timeout.ms", "10000");
        for (k, v) in &p.properties {
            config.set(k, v);
        }
        if p.security_protocol.starts_with("SASL") {
            ensure!(
                ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"].contains(&p.sasl_mechanism.as_str()),
                tr("Use PLAIN ou SCRAM")
            );
            ensure!(!p.username.is_empty(), tr("Informe o usuario SASL"));
            let password = if p.password_env.is_empty() {
                p.password.clone()
            } else {
                std::env::var(&p.password_env)
                    .context(tr("Variavel de ambiente da senha SASL ausente"))?
            };
            config
                .set("sasl.mechanism", &p.sasl_mechanism)
                .set("sasl.username", &p.username)
                .set("sasl.password", password);
        }
        for (key, value) in [
            ("ssl.ca.location", &p.ca_path),
            ("ssl.certificate.location", &p.certificate_path),
            ("ssl.key.location", &p.private_key_path),
        ] {
            if !value.is_empty() {
                config.set(key, value);
            }
        }
        Ok(config)
    }
    fn consumer(&self, group: Option<&str>) -> Result<BaseConsumer> {
        let mut config = self.config()?;
        config
            .set(
                "group.id",
                group
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("kafunekafka-view-{}", uuid::Uuid::new_v4())),
            )
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("enable.partition.eof", "true")
            .set("allow.auto.create.topics", "false")
            .set("isolation.level", "read_committed");
        config
            .set("queued.min.messages", "10000")
            .set("queued.max.messages.kbytes", "16384")
            .set("fetch.queue.backoff.ms", "10")
            .set("fetch.wait.max.ms", "10");
        config
            .create()
            .context(tr("Nao foi possivel criar o cliente Kafka"))
    }
    pub fn snapshot(&self) -> Result<ClusterSnapshot> {
        let consumer = self.consumer(None)?;
        let meta = consumer
            .fetch_metadata(None, Duration::from_secs(10))
            .context(tr("Falha ao conectar ao cluster"))?;
        let brokers = meta
            .brokers()
            .iter()
            .map(|b| BrokerInfo {
                id: b.id(),
                host: b.host().into(),
                port: b.port(),
            })
            .collect();
        let mut snapshot = ClusterSnapshot {
            brokers,
            ..Default::default()
        };
        for topic in meta.topics() {
            if let Some(error) = topic.error() {
                snapshot
                    .warnings
                    .push(format!("{}: {error:?}", topic.name()));
                continue;
            }
            snapshot.topics.push(TopicInfo {
                name: topic.name().into(),
                partitions: topic
                    .partitions()
                    .iter()
                    .map(|p| PartitionInfo {
                        id: p.id(),
                        leader: p.leader(),
                        replicas: p.replicas().to_vec(),
                        in_sync: p.isr().to_vec(),
                    })
                    .collect(),
            });
        }
        snapshot.topics.sort_by(|a, b| a.name.cmp(&b.name));
        match consumer.fetch_group_list(None, Duration::from_secs(10)) {
            Ok(groups) => {
                snapshot.groups = groups
                    .groups()
                    .iter()
                    .map(|g| GroupInfo {
                        name: g.name().into(),
                        state: g.state().into(),
                        protocol: g.protocol().into(),
                        members: g
                            .members()
                            .iter()
                            .map(|m| MemberInfo {
                                id: m.id().into(),
                                client_id: m.client_id().into(),
                                host: m.client_host().into(),
                            })
                            .collect(),
                    })
                    .collect();
                snapshot.groups.sort_by(|a, b| a.name.cmp(&b.name));
            }
            Err(e) => snapshot
                .warnings
                .push(tf!("Grupos nao puderam ser listados: {0}", e)),
        }
        Ok(snapshot)
    }
    pub fn group_offsets(&self, group: &str, topics: &[String]) -> Result<Vec<GroupOffset>> {
        ensure!(!group.is_empty(), tr("Selecione um consumer group"));
        let consumer = self.consumer(Some(group))?;
        let metadata = consumer.fetch_metadata(None, Duration::from_secs(10))?;
        let mut requested = TopicPartitionList::new();
        for topic in metadata.topics() {
            if topic.error().is_some()
                || topic.name().starts_with("__")
                || !topics.is_empty() && !topics.iter().any(|t| t == topic.name())
            {
                continue;
            }
            for p in topic.partitions() {
                requested.add_partition(topic.name(), p.id());
            }
        }
        let committed = consumer
            .committed_offsets(requested, Duration::from_secs(10))
            .context(tr("Falha ao ler offsets do grupo"))?;
        let mut result = vec![];
        for entry in committed.elements() {
            entry.error().context(tr("Erro de offset na particao"))?;
            let offset = match entry.offset() {
                Offset::Offset(n) if n >= 0 => Some(n),
                _ => None,
            };
            let (low, high) = consumer.fetch_watermarks(
                entry.topic(),
                entry.partition(),
                Duration::from_secs(5),
            )?;
            result.push(GroupOffset {
                topic: entry.topic().into(),
                partition: entry.partition(),
                committed: offset,
                low,
                high,
                lag: offset.map(|n| (high - n).max(0)),
            });
        }
        result.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
        Ok(result)
    }
    fn partitions(&self, consumer: &BaseConsumer, topic: &str) -> Result<Vec<i32>> {
        let meta = consumer.fetch_metadata(Some(topic), Duration::from_secs(10))?;
        let entry = meta
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .context(tr("Topico nao encontrado"))?;
        ensure!(
            entry.error().is_none(),
            tf!(
                "Topico {0} indisponivel: {1}",
                topic,
                format!("{:?}", entry.error())
            )
        );
        Ok(entry.partitions().iter().map(|p| p.id()).collect())
    }
    pub fn scan(
        &self,
        options: &ReadOptions,
        cancel: &Cancellation,
        progress: &dyn Fn(Progress),
        mut visit: impl FnMut(KafkaRecord) -> Result<bool>,
    ) -> Result<OperationReport> {
        self.scan_borrowed(options, cancel, progress, |message| {
            visit(owned_record(message))
        })
    }
    fn scan_borrowed(
        &self,
        options: &ReadOptions,
        cancel: &Cancellation,
        progress: &dyn Fn(Progress),
        visit: impl FnMut(&BorrowedMessage<'_>) -> Result<bool>,
    ) -> Result<OperationReport> {
        self.scan_validated(options, cancel, progress, |_| Ok(()), visit)
    }
    fn scan_validated(
        &self,
        options: &ReadOptions,
        cancel: &Cancellation,
        progress: &dyn Fn(Progress),
        before_read: impl FnOnce(&ScanBounds) -> Result<()>,
        mut visit: impl FnMut(&BorrowedMessage<'_>) -> Result<bool>,
    ) -> Result<OperationReport> {
        let trace = crate::perf::Trace::new("scan");
        ensure!(
            !options.topics.is_empty(),
            tr("Selecione um ou mais topicos")
        );
        ensure!(
            options.max_messages > 0 && options.max_scanned > 0 && options.timeout_seconds > 0,
            tr("Limites de leitura precisam ser positivos")
        );
        // One bounded reader per backend. Re-resolve credentials on every read;
        // never reuse a client whose effective configuration has changed.
        let config = self.config()?;
        let values: BTreeMap<String, String> = config
            .config_map()
            .keys()
            .map(|key| ((*key).to_owned(), config.get(key).unwrap().to_owned()))
            .collect();
        let mut reader = self
            .reader
            .lock()
            .map_err(|_| anyhow::anyhow!(tr("Cliente de leitura interrompido")))?;
        if reader
            .as_ref()
            .is_none_or(|session| session.config != values)
        {
            *reader = None;
            *reader = Some(ReadSession {
                consumer: self.consumer(None)?,
                config: values,
            });
            trace.mark("consumer_created");
        } else {
            trace.mark("consumer_reused");
        }
        let consumer = &reader.as_ref().unwrap().consumer;
        let result = self.scan_consumer(
            consumer,
            options,
            cancel,
            progress,
            before_read,
            &mut visit,
            &trace,
        );
        // Unassign on success, cancellation and error: stops prefetch and clears
        // the previous range. Metadata and watermarks are fetched afresh below.
        let cleanup = consumer.unassign();
        trace.mark("unassigned");
        if result.is_err() || cleanup.is_err() {
            *reader = None;
            trace.mark("failed_consumer_dropped");
        }
        let report = result?;
        cleanup.context(tr("Falha ao encerrar a faixa de leitura"))?;
        Ok(report)
    }
    #[allow(clippy::too_many_arguments)]
    fn scan_consumer(
        &self,
        consumer: &BaseConsumer,
        options: &ReadOptions,
        cancel: &Cancellation,
        progress: &dyn Fn(Progress),
        before_read: impl FnOnce(&ScanBounds) -> Result<()>,
        mut visit: impl FnMut(&BorrowedMessage<'_>) -> Result<bool>,
        trace: &crate::perf::Trace,
    ) -> Result<OperationReport> {
        let mut assignment = TopicPartitionList::new();
        let mut bounds = BTreeMap::<(String, i32), (i64, i64)>::new();
        let mut finished = BTreeSet::new();
        for topic in &options.topics {
            let partitions = self.partitions(consumer, topic)?;
            trace.mark("metadata_received");
            if !options.partitions.is_empty() {
                ensure!(
                    options.partitions.iter().all(|p| partitions.contains(p)),
                    tf!("Particao solicitada nao existe em {0}", topic)
                );
            }
            for p in partitions
                .into_iter()
                .filter(|p| options.partitions.is_empty() || options.partitions.contains(p))
            {
                if cancel.load(Ordering::Relaxed) {
                    return Ok(OperationReport {
                        cancelled: true,
                        message: tr("Cancelado antes da leitura").into(),
                        ..Default::default()
                    });
                }
                let (low, high) = consumer.fetch_watermarks(topic, p, Duration::from_secs(5))?;
                trace.mark("watermarks_received");
                let start = match options.start {
                    StartPosition::Earliest => low,
                    StartPosition::Latest(n) => (high - n as i64).max(low),
                    StartPosition::Offset(n) => {
                        ensure!(n >= 0, tr("Offset precisa ser positivo"));
                        n.max(low)
                    }
                    StartPosition::Timestamp(timestamp) => {
                        let mut times = TopicPartitionList::new();
                        times.add_partition_offset(topic, p, Offset::Offset(timestamp))?;
                        let found = consumer.offsets_for_times(times, Duration::from_secs(5))?;
                        let entry = found
                            .find_partition(topic, p)
                            .context(tr("Particao sem resposta de timestamp"))?;
                        entry.error()?;
                        match entry.offset() {
                            Offset::Offset(n) if n >= 0 => n.max(low),
                            _ => high,
                        }
                    }
                };
                let key = (topic.clone(), p);
                if start >= high {
                    finished.insert(key.clone());
                } else {
                    assignment.add_partition_offset(topic, p, Offset::Offset(start))?;
                }
                bounds.insert(key, (start, high));
            }
        }
        before_read(&bounds)?;
        consumer.assign(&assignment)?;
        trace.mark("assigned");
        let deadline = Instant::now() + Duration::from_secs(options.timeout_seconds);
        let mut report = OperationReport::default();
        let mut last_progress = Instant::now();
        while finished.len() < bounds.len() {
            if cancel.load(Ordering::Relaxed) {
                report.cancelled = true;
                report.message = tr("Cancelado; resultado parcial").into();
                break;
            }
            if Instant::now() >= deadline {
                report.message = tr("Tempo limite atingido; resultado parcial").into();
                break;
            }
            if report.scanned >= options.max_scanned {
                report.message = tr("Limite de mensagens examinadas atingido").into();
                break;
            }
            match consumer.poll(Duration::from_millis(100)) {
                Some(Ok(message)) => {
                    if report.scanned == 0 {
                        trace.mark("first_record");
                    }
                    let key = (message.topic().to_owned(), message.partition());
                    let Some(&(start, end)) = bounds.get(&key) else {
                        continue;
                    };
                    if finished.contains(&key) {
                        continue;
                    }
                    if message.offset() >= end {
                        finished.insert(key);
                        continue;
                    }
                    if message.offset() < start {
                        continue;
                    }
                    report.scanned += 1;
                    if visit(&message)? {
                        report.matched += 1;
                    }
                    if message.offset() + 1 >= end {
                        finished.insert(key);
                    }
                    if report.matched >= options.max_messages {
                        report.message = tr("Limite de resultados atingido").into();
                        break;
                    }
                }
                Some(Err(KafkaError::PartitionEOF(_))) => {
                    // Position is per topic-partition; the EOF error alone contains only a partition id.
                    let positions = consumer.position()?;
                    for entry in positions.elements() {
                        let key = (entry.topic().to_owned(), entry.partition());
                        if let Some(&(_, end)) = bounds.get(&key)
                            && matches!(entry.offset(),Offset::Offset(n) if n>=end)
                        {
                            finished.insert(key);
                        }
                    }
                }
                Some(Err(e)) => {
                    return Err(e).context(tr("Falha na leitura Kafka; operacao nao concluida"));
                }
                None => (),
            }
            if last_progress.elapsed() >= Duration::from_millis(200) {
                progress(Progress {
                    scanned: report.scanned,
                    matched: report.matched,
                    delivered: 0,
                    phase: tr("Lendo snapshot de offsets").into(),
                });
                last_progress = Instant::now();
            }
        }
        report.complete = finished.len() == bounds.len();
        if report.complete && report.message.is_empty() {
            report.message = tr("Snapshot lido ate o fim").into();
        }
        progress(Progress {
            scanned: report.scanned,
            matched: report.matched,
            delivered: 0,
            phase: report.message.clone(),
        });
        trace.mark("records_collected");
        Ok(report)
    }
    pub fn read(
        &self,
        options: &ReadOptions,
        cancel: &Cancellation,
        progress: &dyn Fn(Progress),
    ) -> Result<ScanResult> {
        let mut records = vec![];
        let mut retained_bytes = 0usize;
        let report = self.scan(options, cancel, progress, |r| {
            retained_bytes += record_bytes(&r);
            ensure!(retained_bytes <= 128 * 1024 * 1024, tr("A leitura ultrapassou 128 MiB. Reduza a faixa ou use exportacao, que grava os dados em fluxo."));
            records.push(r);
            Ok(true)
        })?;
        Ok(ScanResult { records, report })
    }
    pub fn search(
        &self,
        options: &ReadOptions,
        search: &SearchOptions,
        cancel: &Cancellation,
        progress: &dyn Fn(Progress),
    ) -> Result<ScanResult> {
        ensure!(
            !search.pattern.is_empty(),
            tr("Informe o texto ou regex da busca")
        );
        ensure!(
            search.keys || search.values || search.headers,
            tr("Escolha onde procurar")
        );
        let pattern = if search.regex {
            search.pattern.clone()
        } else {
            regex::escape(&search.pattern)
        };
        let regex = RegexBuilder::new(&pattern)
            .case_insensitive(!search.case_sensitive)
            .size_limit(10 * 1024 * 1024)
            .build()
            .context(tr("Regex invalida"))?;
        let mut decoder = Decoder::new(&self.profile)?;
        let mut records = vec![];
        let mut retained_bytes = 0usize;
        let report = self.scan_borrowed(options, cancel, progress, |message| {
            let mut matches_bytes = |bytes| {
                let text = decoder
                    .display(bytes, DisplayFormat::Auto)
                    .unwrap_or_else(|_| {
                        String::from_utf8_lossy(bytes.unwrap_or_default()).into_owned()
                    });
                regex.is_match(&text)
            };
            let matched = search.keys && matches_bytes(message.key())
                || search.values && matches_bytes(message.payload())
                || search.headers
                    && message.headers().is_some_and(|h| {
                        (0..h.count()).any(|i| {
                            let header = h.get(i);
                            regex.is_match(header.key)
                                || regex.is_match(&String::from_utf8_lossy(
                                    header.value.unwrap_or_default(),
                                ))
                        })
                    });
            if matched {
                let r = owned_record(message);
                retained_bytes += record_bytes(&r);
                ensure!(
                    retained_bytes <= 128 * 1024 * 1024,
                    tr("Os resultados ultrapassaram 128 MiB. Reduza a faixa ou refine a busca.")
                );
                records.push(r);
            }
            Ok(matched)
        })?;
        Ok(ScanResult { records, report })
    }
    fn producer_config(&self) -> Result<ClientConfig> {
        let mut config = self.config()?;
        config
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("delivery.report.only.error", "false")
            .set("request.timeout.ms", "10000")
            .set("message.timeout.ms", "30000");
        Ok(config)
    }
    fn producer(&self) -> Result<FutureProducer> {
        Ok(self.producer_config()?.create()?)
    }
    fn validate_destination(&self, topic: &str, partition: Option<i32>) -> Result<Vec<i32>> {
        ensure!(!topic.trim().is_empty(), tr("Informe o topico de destino"));
        let consumer = self.consumer(None)?;
        let parts = self.partitions(&consumer, topic)?;
        if let Some(p) = partition {
            ensure!(
                parts.contains(&p),
                tf!("Particao {0} nao existe no destino", p)
            );
        }
        Ok(parts)
    }
    fn produce(
        producer: &FutureProducer,
        runtime: &tokio::runtime::Runtime,
        r: &ProduceRecord,
    ) -> Result<(i32, i64)> {
        let message = future_record(r);
        runtime.block_on(producer.send(message,Duration::from_secs(5))).map(|d|(d.partition,d.offset)).map_err(|(e,_)| anyhow::anyhow!(tf!("Kafka nao confirmou a mensagem: {0}. Em timeout, verifique o destino antes de repetir.", e)))
    }
    pub fn send(&self, record: &ProduceRecord) -> Result<(i32, i64)> {
        self.validate_destination(&record.topic, record.partition)?;
        let producer = self.producer()?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        Self::produce(&producer, &runtime, record)
    }
    pub fn export(
        &self,
        options: &ReadOptions,
        path: &Path,
        cancel: &Cancellation,
        progress: &dyn Fn(Progress),
    ) -> Result<OperationReport> {
        let mut archive = storage::AtomicArchive::new(path)?;
        let result = self.scan(options, cancel, progress, |r| {
            archive.write(&r)?;
            Ok(true)
        });
        match result {
            Ok(mut report) => {
                let saved = if report.cancelled
                    || !report.complete && !report.message.starts_with(tr("Limite de resultados"))
                {
                    archive.preserve_partial()?
                } else {
                    archive.finish()?
                };
                report.message = tf!(
                    "{0} registros salvos em {1}. {2}",
                    report.matched,
                    saved.display(),
                    report.message
                );
                Ok(report)
            }
            Err(error) => {
                let partial = archive.preserve_partial()?;
                Err(error).context(tf!(
                    "Exportacao interrompida; prefixo preservado em {0}",
                    partial.display()
                ))
            }
        }
    }
    pub fn import(
        &self,
        path: &Path,
        topic: &str,
        preserve_partition: bool,
        preserve_timestamp: bool,
        cancel: &Cancellation,
        progress: &dyn Fn(Progress),
    ) -> Result<OperationReport> {
        let parts = self.validate_destination(topic, None)?;
        let mut total = 0;
        storage::visit_archive(path, |r| {
            if preserve_partition {
                ensure!(
                    parts.contains(&r.partition),
                    tf!("Particao {0} do arquivo nao existe no destino", r.partition)
                );
            }
            total += 1;
            Ok(!cancel.load(Ordering::Relaxed))
        })?;
        if cancel.load(Ordering::Relaxed) {
            return Ok(OperationReport {
                cancelled: true,
                message: tr("Importacao cancelada antes de publicar").into(),
                ..Default::default()
            });
        }
        ensure!(total > 0, tr("Arquivo nao contem mensagens"));
        let mut window = DeliveryWindow::new(self.producer_config()?)?;
        let mut report = OperationReport::default();
        let mut last_progress = Instant::now();
        let result = storage::visit_archive(path, |r| {
            if cancel.load(Ordering::Relaxed) {
                report.cancelled = true;
                return Ok(false);
            }
            let mut record = ProduceRecord::from(r);
            record.topic = topic.into();
            if !preserve_partition {
                record.partition = None;
            }
            if !preserve_timestamp {
                record.timestamp = None;
            }
            window.enqueue(&record)?;
            report.scanned += 1;
            if report.scanned == 1 || last_progress.elapsed() >= Duration::from_millis(200) {
                progress(Progress {
                    scanned: report.scanned,
                    delivered: window.delivered,
                    phase: tf!("Importando {0}/{1}", window.delivered, total),
                    ..Default::default()
                });
                last_progress = Instant::now();
            }
            Ok(true)
        });
        let drained = window.finish();
        result.and(drained.map(|_| 0)).with_context(|| {
            tf!(
                "Importacao parcial: {0} de {1} mensagens confirmadas",
                window.delivered,
                total
            )
        })?;
        report.delivered = window.delivered;
        report.destination_offsets = window.receipts;
        report.cancelled |= cancel.load(Ordering::Relaxed) && report.delivered < total;
        report.complete = !report.cancelled && report.delivered == total;
        report.message = tf!(
            "{0} de {1} mensagens confirmadas{2}",
            report.delivered,
            total,
            if report.cancelled {
                tr("; cancelado")
            } else {
                ""
            }
        );
        progress(Progress {
            delivered: report.delivered,
            phase: report.message.clone(),
            ..Default::default()
        });
        Ok(report)
    }
    pub fn replay(
        &self,
        options: &ReadOptions,
        destination: &Backend,
        replay: &ReplayOptions,
        cancel: &Cancellation,
        progress: &dyn Fn(Progress),
    ) -> Result<OperationReport> {
        let same_cluster = self.profile.id == destination.profile.id
            || self.profile.bootstrap_servers == destination.profile.bootstrap_servers;
        ensure!(
            !same_cluster || !options.topics.contains(&replay.destination_topic),
            tr("Replay exige um topico de destino diferente da origem")
        );
        let parts = destination.validate_destination(&replay.destination_topic, None)?;
        let mut window = DeliveryWindow::new(destination.producer_config()?)?;
        let mut last_progress = Instant::now();
        let result = self.scan_validated(
            options,
            cancel,
            progress,
            |bounds| {
                ensure!(
                    !replay.preserve_partition || bounds.keys().all(|(_, p)| parts.contains(p)),
                    tr("Destino nao possui todas as particoes selecionadas")
                );
                Ok(())
            },
            |message| {
                let mut record = ProduceRecord::from(owned_record(message));
                record.topic = replay.destination_topic.clone();
                if !replay.preserve_partition {
                    record.partition = None;
                }
                if !replay.preserve_timestamp {
                    record.timestamp = None;
                }
                window.enqueue(&record)?;
                if last_progress.elapsed() >= Duration::from_millis(200) {
                    progress(Progress {
                        delivered: window.delivered,
                        phase: tr("Reproduzindo no destino").into(),
                        ..Default::default()
                    });
                    last_progress = Instant::now();
                }
                Ok(true)
            },
        );
        let drained = window.finish();
        let mut report = result.with_context(|| {
            tf!(
                "Replay parcial: {0} mensagens confirmadas",
                window.delivered
            )
        })?;
        drained?;
        report.delivered = window.delivered;
        report.destination_offsets = window.receipts;
        report.message = tf!(
            "{0} mensagens confirmadas no destino. {1}",
            report.delivered,
            report.message
        );
        progress(Progress {
            scanned: report.scanned,
            matched: report.matched,
            delivered: report.delivered,
            phase: report.message.clone(),
        });
        Ok(report)
    }
    pub fn create_fixture_topic(&self, name: &str, partitions: i32) -> Result<()> {
        use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
        let admin: AdminClient<_> = self.config()?.create()?;
        let rt = tokio::runtime::Runtime::new()?;
        let results = rt.block_on(admin.create_topics(
            &[NewTopic::new(name, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        ))?;
        for result in results {
            if let Err((_, e)) = result
                && e != RDKafkaErrorCode::TopicAlreadyExists
            {
                bail!(tf!("Falha ao criar fixture: {0}", format!("{:?}", e)));
            }
        }
        let consumer = self.consumer(None)?;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match self.partitions(&consumer, name) {
                Ok(parts) if parts.len() == partitions as usize => break,
                result if Instant::now() >= deadline => {
                    result.context(tr(
                        "A fixture foi criada, mas seus metadados nao ficaram disponiveis a tempo",
                    ))?;
                    bail!(tr(
                        "Numero de particoes da fixture ainda nao corresponde ao solicitado"
                    ));
                }
                _ => std::thread::sleep(Duration::from_millis(100)),
            }
        }
        Ok(())
    }
}

fn record_bytes(record: &KafkaRecord) -> usize {
    record.key.as_ref().map_or(0, Vec::len)
        + record.value.as_ref().map_or(0, Vec::len)
        + record
            .headers
            .iter()
            .map(|h| {
                h.key.len()
                    + h.value.as_ref().map_or(0, Vec::len)
                    + std::mem::size_of::<RecordHeader>()
            })
            .sum::<usize>()
        + record.topic.len()
        + std::mem::size_of::<KafkaRecord>()
}

fn owned_record(message: &BorrowedMessage<'_>) -> KafkaRecord {
    KafkaRecord {
        topic: message.topic().into(),
        partition: message.partition(),
        offset: message.offset(),
        timestamp: message.timestamp().to_millis(),
        key: message.key().map(<[u8]>::to_vec),
        value: message.payload().map(<[u8]>::to_vec),
        headers: message
            .headers()
            .map(|h| {
                (0..h.count())
                    .map(|i| {
                        let header = h.get(i);
                        RecordHeader {
                            key: header.key.into(),
                            value: header.value.map(<[u8]>::to_vec),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default(),
    }
}
