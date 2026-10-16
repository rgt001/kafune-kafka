use crate::{
    backend::{Backend, Cancellation, cancellation},
    codec::{Decoder, DisplayFormat},
    i18n::{Language, UiSettings},
    model::*,
    storage,
};
use crate::{i18n::tr, tf};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use eframe::egui::{self, Color32, RichText, TextEdit};
use egui_extras::{Column, TableBuilder};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, atomic::Ordering, mpsc},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Tab {
    Cluster,
    Messages,
    Consumers,
    Compose,
    Transfer,
    Search,
}
impl Tab {
    fn label(self) -> &'static str {
        match self {
            Self::Cluster => tr("Cluster"),
            Self::Messages => tr("Mensagens"),
            Self::Consumers => tr("Consumidores"),
            Self::Compose => tr("Enviar / Reenviar"),
            Self::Transfer => tr("Importar / Exportar / Replay"),
            Self::Search => tr("Buscar"),
        }
    }
}
enum JobResult {
    Snapshots(Vec<(String, std::result::Result<ClusterSnapshot, String>)>),
    Records(ScanResult, Option<(String, String)>),
    Offsets(Vec<GroupOffset>),
    Report(OperationReport),
    Published(String, i32, i64),
    Decoded(usize, String, String),
    Saved(String),
}
enum Event {
    Progress(Progress),
    Done(std::result::Result<JobResult, String>, crate::perf::Trace),
}
type Work = Box<dyn FnOnce(Cancellation, &dyn Fn(Progress)) -> Result<JobResult> + Send>;
struct Job {
    work: Work,
    language: Language,
    cancel: Cancellation,
    ctx: egui::Context,
    trace: crate::perf::Trace,
}

fn job_worker(jobs: mpsc::Receiver<Job>, sender: mpsc::Sender<Event>) {
    while let Ok(Job {
        work,
        language,
        cancel,
        ctx,
        trace,
    }) = jobs.recv()
    {
        trace.mark("worker_started");
        let progress = |p| {
            let _ = sender.send(Event::Progress(p));
            ctx.request_repaint();
        };
        let result = language.with(|| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work(cancel, &progress)))
                .map_err(|_| {
                    tr("A tarefa falhou inesperadamente; nenhuma conclusao foi assumida").into()
                })
                .and_then(|r| r.map_err(|e| format!("{e:#}")))
        });
        trace.mark("work_returned");
        if sender.send(Event::Done(result, trace)).is_err() {
            break;
        }
        ctx.request_repaint();
    }
}
#[derive(Clone, Default)]
struct Composer {
    topic: String,
    partition: String,
    key: String,
    value: String,
    key_mode: usize,
    value_mode: usize,
    key_null: bool,
    value_null: bool,
    headers: String,
    timestamp: String,
    schema: String,
    schema_id: String,
    source: Option<(String, i32, i64)>,
}
struct PendingSave {
    record: KafkaRecord,
    path: String,
    raw: bool,
}

pub struct KafuneKafkaApp {
    pub profiles: Vec<ClusterProfile>,
    pub snapshots: BTreeMap<String, ClusterSnapshot>,
    selected_cluster: usize,
    selected_topics: BTreeSet<String>,
    topic_filter: String,
    partitions: String,
    pub tab: Tab,
    start_mode: usize,
    start_value: String,
    limit: usize,
    scan_limit: usize,
    timeout: u64,
    pub records: Vec<KafkaRecord>,
    selected_record: Option<usize>,
    format: DisplayFormat,
    key_display: String,
    value_display: String,
    group: String,
    offsets: Vec<GroupOffset>,
    search: SearchOptions,
    composer: Composer,
    destination_cluster: usize,
    destination_topic: String,
    import_path: String,
    export_path: String,
    keep_partition: bool,
    keep_timestamp: bool,
    profile_edit: Option<ClusterProfile>,
    pending_save: Option<PendingSave>,
    properties_json: String,
    busy: bool,
    cancel: Cancellation,
    jobs: mpsc::Sender<Job>,
    receiver: mpsc::Receiver<Event>,
    progress: Progress,
    status: String,
    error: Option<String>,
    logs: Vec<String>,
    config_path: PathBuf,
    settings_path: PathBuf,
    language: Language,
    smoke_output: Option<PathBuf>,
    smoke_started: bool,
    smoke_read_started: Option<Instant>,
    smoke_trial: usize,
    smoke_remaining: usize,
    smoke_deadline: Instant,
    screenshot_requested: bool,
    read_backend: Option<Arc<Backend>>,
}
impl KafuneKafkaApp {
    pub fn new(cc: &eframe::CreationContext<'_>, smoke: Option<PathBuf>) -> Self {
        cc.egui_ctx.set_theme(egui::ThemePreference::Dark);
        cc.egui_ctx.set_visuals(egui::Visuals::dark());
        let mut style = (*cc.egui_ctx.style()).clone();
        style.spacing.item_spacing = egui::vec2(10., 8.);
        style.spacing.button_padding = egui::vec2(12., 7.);
        cc.egui_ctx.set_style(style);
        Self::create(smoke)
    }
    pub fn create(smoke: Option<PathBuf>) -> Self {
        let config_path = std::env::var_os("KAFUNEKAFKA_CONFIG")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                storage::profile_path().unwrap_or_else(|_| PathBuf::from("clusters.json"))
            });
        let settings_path = config_path.with_file_name("settings.json");
        let (settings, settings_error) = match storage::load_settings(&settings_path) {
            Ok(settings) => (settings, None),
            Err(error) => (UiSettings::default(), Some(format!("{error:#}"))),
        };
        let language = std::env::var("KAFUNEKAFKA_LANGUAGE")
            .ok()
            .and_then(|code| Language::from_code(&code))
            .unwrap_or(settings.language);
        let (profiles, error) = match language.with(|| storage::load_profiles(&config_path)) {
            Ok(p) => (p, None),
            Err(e) => (vec![], Some(format!("{e:#}"))),
        };
        let (sender, receiver) = mpsc::channel();
        let (jobs, job_receiver) = mpsc::channel();
        let worker_error = std::thread::Builder::new()
            .name("kafunekafka-worker".into())
            .spawn(move || job_worker(job_receiver, sender))
            .err()
            .map(|e| language.with(|| tf!("Falha ao iniciar worker: {0}", e)));
        Self {
            profiles: if profiles.is_empty() {
                vec![ClusterProfile::default()]
            } else {
                profiles
            },
            snapshots: BTreeMap::new(),
            selected_cluster: 0,
            selected_topics: BTreeSet::new(),
            topic_filter: String::new(),
            partitions: String::new(),
            tab: Tab::Cluster,
            start_mode: 1,
            start_value: "100".into(),
            limit: 1000,
            scan_limit: 100_000,
            timeout: 30,
            records: vec![],
            selected_record: None,
            format: DisplayFormat::Auto,
            key_display: String::new(),
            value_display: String::new(),
            group: String::new(),
            offsets: vec![],
            search: SearchOptions::default(),
            composer: Composer {
                headers: "[]".into(),
                key_null: true,
                ..Default::default()
            },
            destination_cluster: 0,
            destination_topic: String::new(),
            import_path: String::new(),
            export_path: String::new(),
            keep_partition: false,
            keep_timestamp: false,
            profile_edit: None,
            pending_save: None,
            properties_json: "{}".into(),
            busy: false,
            cancel: cancellation(),
            jobs,
            receiver,
            progress: Progress::default(),
            status: language.with(|| tr("Selecione um cluster e conecte").into()),
            error: worker_error.or(error).or(settings_error),
            logs: vec![],
            config_path,
            settings_path,
            language,
            smoke_output: smoke,
            smoke_started: false,
            smoke_read_started: None,
            smoke_trial: 0,
            smoke_remaining: std::env::var("KAFUNEKAFKA_SMOKE_REPEATS")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .filter(|n| *n > 0 && *n <= 10)
                .unwrap_or(1),
            smoke_deadline: Instant::now() + Duration::from_secs(60),
            screenshot_requested: false,
            read_backend: None,
        }
    }
    fn profile(&self) -> ClusterProfile {
        self.profiles[self.selected_cluster.min(self.profiles.len() - 1)].clone()
    }
    fn snapshot(&self) -> Option<&ClusterSnapshot> {
        self.snapshots.get(&self.profiles[self.selected_cluster].id)
    }
    fn log(&mut self, text: String) {
        self.status = text.clone();
        self.logs.push(format!(
            "{}  {text}",
            chrono::Local::now().format("%H:%M:%S")
        ));
        if self.logs.len() > 200 {
            self.logs.remove(0);
        }
    }
    fn start(
        &mut self,
        ctx: &egui::Context,
        label: &str,
        work: impl FnOnce(Cancellation, &dyn Fn(Progress)) -> Result<JobResult> + Send + 'static,
    ) {
        if self.busy {
            return;
        }
        self.busy = true;
        self.error = None;
        self.cancel = cancellation();
        self.progress = Progress {
            phase: label.into(),
            ..Default::default()
        };
        if self
            .jobs
            .send(Job {
                work: Box::new(work),
                language: self.language,
                cancel: self.cancel.clone(),
                ctx: ctx.clone(),
                trace: crate::perf::Trace::new("job"),
            })
            .is_err()
        {
            self.busy = false;
            self.error = Some(tr("Worker indisponivel; reabra o aplicativo").into());
        }
    }
    fn connect_all(&mut self, ctx: &egui::Context) {
        if self.busy {
            return;
        }
        let previous_reader = self.read_backend.take();
        let profiles = self.profiles.clone();
        self.start(ctx, tr("Conectando clusters"), move |cancel, progress| {
            // Explicit refresh also reloads certificates replaced at the same
            // path. Dispose of the old connection on the worker, not the UI.
            drop(previous_reader);
            let mut snapshots = vec![];
            for profile in profiles {
                if cancel.load(Ordering::Relaxed) {
                    break;
                }
                progress(Progress {
                    phase: tf!("Conectando {0}", profile.name),
                    ..Default::default()
                });
                let id = profile.id.clone();
                snapshots.push((
                    id,
                    Backend::new(profile)
                        .snapshot()
                        .map_err(|e| format!("{e:#}")),
                ));
            }
            Ok(JobResult::Snapshots(snapshots))
        });
    }
    fn options(&self) -> Result<ReadOptions> {
        ensure!(
            !self.selected_topics.is_empty(),
            tr("Selecione topicos na barra lateral")
        );
        let partitions = if self.partitions.trim().is_empty() {
            vec![]
        } else {
            self.partitions
                .split(',')
                .map(|v| {
                    v.trim()
                        .parse::<i32>()
                        .context(tr("Particoes: use numeros separados por virgula"))
                })
                .collect::<Result<Vec<_>>>()?
        };
        let start = match self.start_mode {
            0 => StartPosition::Earliest,
            1 => StartPosition::Latest(
                self.start_value
                    .parse()
                    .context(tr("Quantidade de mensagens finais invalida"))?,
            ),
            2 => StartPosition::Offset(self.start_value.parse().context(tr("Offset invalido"))?),
            _ => StartPosition::Timestamp(
                self.start_value
                    .parse()
                    .context(tr("Timestamp invalido; use milissegundos Unix"))?,
            ),
        };
        Ok(ReadOptions {
            topics: self.selected_topics.iter().cloned().collect(),
            partitions,
            start,
            max_messages: self.limit,
            max_scanned: self.scan_limit,
            timeout_seconds: self.timeout,
        })
    }
    fn load_messages(&mut self, ctx: &egui::Context, search: bool) {
        let options = match self.options() {
            Ok(o) => o,
            Err(e) => {
                self.error = Some(e.to_string());
                return;
            }
        };
        if options.max_messages > 20_000 {
            self.error = Some(
                tr("A tabela aceita ate 20.000 resultados. Use exportacao para volumes maiores.")
                    .into(),
            );
            return;
        }
        let profile = self.profile();
        if self
            .read_backend
            .as_ref()
            .is_none_or(|b| b.profile != profile)
        {
            self.read_backend = Some(Arc::new(Backend::new(profile.clone())));
        }
        let backend = self.read_backend.as_ref().unwrap().clone();
        let filter = self.search.clone();
        let format = self.format;
        self.start(
            ctx,
            if search {
                tr("Buscando no Kafka")
            } else {
                tr("Lendo mensagens")
            },
            move |cancel, progress| {
                let trace = crate::perf::Trace::new("read_worker");
                let result = if search {
                    backend.search(&options, &filter, &cancel, progress)?
                } else {
                    backend.read(&options, &cancel, progress)?
                };
                trace.mark("read_returned");
                let decoded = result
                    .records
                    .first()
                    .map(|record| {
                        let mut decoder = Decoder::new(&profile)?;
                        Ok::<_, anyhow::Error>(decode_record(&mut decoder, record, format))
                    })
                    .transpose()?;
                trace.mark("first_record_formatted");
                Ok(JobResult::Records(result, decoded))
            },
        );
    }
    fn decode_selected(&mut self, ctx: &egui::Context) {
        let Some(index) = self.selected_record else {
            return;
        };
        let Some(record) = self.records.get(index).cloned() else {
            return;
        };
        let profile = self.profile();
        let format = self.format;
        self.start(ctx, tr("Formatando mensagem"), move |_, _| {
            let trace = crate::perf::Trace::new("decode_worker");
            let mut decoder = Decoder::new(&profile)?;
            trace.mark("decoder_created");
            let (key, value) = decode_record(&mut decoder, &record, format);
            trace.mark("formatted");
            drop(decoder);
            trace.mark("decoder_dropped");
            Ok(JobResult::Decoded(index, key, value))
        });
    }
    fn receive(&mut self) {
        let events: Vec<_> = self.receiver.try_iter().collect();
        for event in events {
            match event {
                Event::Progress(p) => {
                    self.progress.scanned = self.progress.scanned.max(p.scanned);
                    self.progress.matched = self.progress.matched.max(p.matched);
                    self.progress.delivered = self.progress.delivered.max(p.delivered);
                    self.progress.phase = p.phase;
                }
                Event::Done(result, trace) => {
                    trace.mark("ui_received");
                    self.busy = false;
                    match result {
                        Err(error) => {
                            self.log(tf!("Erro: {0}", error));
                            self.error = Some(error);
                        }
                        Ok(JobResult::Snapshots(results)) => {
                            for (id, result) in results {
                                match result {
                                    Ok(snapshot) => {
                                        for warning in &snapshot.warnings {
                                            self.log(warning.clone());
                                        }
                                        self.snapshots.insert(id, snapshot);
                                    }
                                    Err(error) => {
                                        self.snapshots.remove(&id);
                                        self.error = Some(error.clone());
                                        self.log(error);
                                    }
                                }
                            }
                            self.log(tf!("{0} clusters conectados", self.snapshots.len()));
                        }
                        Ok(JobResult::Records(result, decoded)) => {
                            self.records = result.records;
                            self.selected_record = if self.records.is_empty() {
                                None
                            } else {
                                Some(0)
                            };
                            (self.key_display, self.value_display) = decoded.unwrap_or_default();
                            self.log(tf!(
                                "{0} mensagens exibidas; {1} examinadas. {2}",
                                self.records.len(),
                                result.report.scanned,
                                result.report.message
                            ));
                        }
                        Ok(JobResult::Offsets(offsets)) => {
                            self.offsets = offsets;
                            self.log(tf!("{0} offsets consultados", self.offsets.len()));
                        }
                        Ok(JobResult::Report(report)) => self.log(report.message),
                        Ok(JobResult::Published(topic, p, o)) => self.log(tf!(
                            "Mensagem confirmada: {0} / particao {1} / offset {2}",
                            topic,
                            p,
                            o
                        )),
                        Ok(JobResult::Decoded(index, key, value)) => {
                            if self.selected_record == Some(index) {
                                self.key_display = key;
                                self.value_display = value;
                            }
                        }
                        Ok(JobResult::Saved(path)) => self.log(tf!("Arquivo salvo: {0}", path)),
                    }
                }
            }
        }
    }
    fn read_controls(&mut self, ui: &mut egui::Ui) {
        ui.horizontal_wrapped(|ui| {
            egui::ComboBox::from_id_salt("start")
                .selected_text(
                    [
                        tr("Inicio"),
                        tr("Ultimas N por particao"),
                        tr("Offset"),
                        tr("Timestamp Unix ms"),
                    ][self.start_mode],
                )
                .show_ui(ui, |ui| {
                    for (i, label) in [
                        tr("Inicio"),
                        tr("Ultimas N por particao"),
                        tr("Offset"),
                        tr("Timestamp Unix ms"),
                    ]
                    .iter()
                    .enumerate()
                    {
                        ui.selectable_value(&mut self.start_mode, i, *label);
                    }
                });
            if self.start_mode != 0 {
                ui.add(TextEdit::singleline(&mut self.start_value).desired_width(145.));
            }
            ui.label(tr("Particoes"));
            ui.add(
                TextEdit::singleline(&mut self.partitions)
                    .hint_text(tr("Todas, ou 0,1,2"))
                    .desired_width(130.),
            );
            ui.label(tr("Resultados"));
            ui.add(egui::DragValue::new(&mut self.limit).range(1..=1_000_000));
        });
        ui.horizontal_wrapped(|ui| {
            ui.label(tr("Examinar ate"));
            ui.add(egui::DragValue::new(&mut self.scan_limit).range(1..=10_000_000));
            ui.label(tr("mensagens / timeout"));
            ui.add(egui::DragValue::new(&mut self.timeout).range(1..=3600));
            ui.label(tr("segundos"));
        });
    }
    fn sidebar(&mut self, ctx: &egui::Context) {
        egui::SidePanel::left("navigation")
            .resizable(true)
            .default_width(245.)
            .show(ctx, |ui| {
                ui.heading(tr("Clusters"));
                ui.add_enabled_ui(!self.busy, |ui| {
                    for (index, p) in self.profiles.iter().enumerate() {
                        let connected = self.snapshots.contains_key(&p.id);
                        ui.horizontal(|ui| {
                            ui.colored_label(
                                if connected {
                                    Color32::from_rgb(80, 210, 160)
                                } else {
                                    Color32::GRAY
                                },
                                "●",
                            );
                            if ui
                                .selectable_label(index == self.selected_cluster, &p.name)
                                .clicked()
                            {
                                self.selected_cluster = index;
                                self.selected_topics.clear();
                                self.records.clear();
                                self.offsets.clear();
                                self.group.clear();
                                self.selected_record = None;
                                self.tab = Tab::Cluster;
                            }
                        });
                    }
                    ui.horizontal(|ui| {
                        if ui.button(tr("+ Novo")).clicked() {
                            self.profile_edit = Some(ClusterProfile::default());
                            self.properties_json = "{}".into();
                        }
                        if ui.button(tr("Editar")).clicked() {
                            let p = self.profile();
                            self.properties_json =
                                serde_json::to_string_pretty(&p.properties).unwrap();
                            self.profile_edit = Some(p);
                        }
                    });
                    if ui.button(tr("Conectar / atualizar todos")).clicked() {
                        self.connect_all(ctx);
                    }
                });
                ui.separator();
                ui.heading(tr("Topicos"));
                ui.add(
                    TextEdit::singleline(&mut self.topic_filter).hint_text(tr("Filtrar topicos")),
                );
                let topics = self
                    .profiles
                    .get(self.selected_cluster)
                    .and_then(|p| self.snapshots.get(&p.id))
                    .map(|s| s.topics.as_slice())
                    .unwrap_or_default();
                let filter = self.topic_filter.to_lowercase();
                let filtered: Vec<_> = topics
                    .iter()
                    .filter(|t| filter.is_empty() || t.name.to_lowercase().contains(&filter))
                    .collect();
                ui.add_enabled_ui(!self.busy, |ui| {
                    egui::ScrollArea::vertical().id_salt("topics").show_rows(
                        ui,
                        ui.spacing().interact_size.y,
                        filtered.len(),
                        |ui, rows| {
                            for index in rows {
                                let topic = filtered[index];
                                let mut selected = self.selected_topics.contains(&topic.name);
                                if ui
                                    .checkbox(
                                        &mut selected,
                                        format!("{}  ({})", topic.name, topic.partitions.len()),
                                    )
                                    .changed()
                                {
                                    if selected {
                                        self.selected_topics.insert(topic.name.clone());
                                    } else {
                                        self.selected_topics.remove(&topic.name);
                                    }
                                }
                            }
                            if topics.is_empty() {
                                ui.weak(tr("Conecte para listar os topicos"));
                            }
                        },
                    );
                });
            });
    }
    fn cluster_view(&mut self, ui: &mut egui::Ui) {
        let Some(snapshot) = self.snapshot() else {
            ui.heading(tr("Seu Kafka, em um lugar"));
            ui.label(tr(
                "Cadastre os clusters, conecte e selecione topicos na barra lateral.",
            ));
            return;
        };
        ui.heading(&self.profile().name);
        ui.horizontal(|ui| {
            ui.label(tf!("{0} brokers", snapshot.brokers.len()));
            ui.separator();
            ui.label(tf!("{0} topicos", snapshot.topics.len()));
            ui.separator();
            ui.label(tf!("{0} consumer groups", snapshot.groups.len()));
        });
        ui.separator();
        ui.heading(tr("Brokers"));
        for b in &snapshot.brokers {
            ui.monospace(format!("#{}  {}:{}", b.id, b.host, b.port));
        }
        ui.separator();
        ui.heading(tr("Topicos e particoes"));
        egui::ScrollArea::vertical().show(ui, |ui| {
            for topic in &snapshot.topics {
                egui::CollapsingHeader::new(tf!(
                    "{0} / {1} particoes",
                    topic.name,
                    topic.partitions.len()
                ))
                .id_salt(&topic.name)
                .show(ui, |ui| {
                    for p in &topic.partitions {
                        ui.monospace(tf!(
                            "Particao {0}   Leader {1}   Replicas {2}   ISR {3}",
                            p.id,
                            p.leader,
                            format!("{:?}", p.replicas),
                            format!("{:?}", p.in_sync)
                        ));
                    }
                });
            }
        });
    }
    fn messages_view(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        ui.add_enabled_ui(!self.busy, |ui| {
            self.read_controls(ui);
            if ui.button(tr("Ler mensagens")).clicked() {
                self.load_messages(ctx, false);
            }
        });
        ui.separator();
        let mut select = None;
        ui.allocate_ui(egui::vec2(ui.available_width(), 280.), |ui| {
            TableBuilder::new(ui)
                .striped(true)
                .resizable(true)
                .max_scroll_height(235.)
                .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                .column(Column::initial(140.))
                .column(Column::initial(65.))
                .column(Column::initial(90.))
                .column(Column::initial(130.))
                .column(Column::initial(120.))
                .column(Column::remainder())
                .header(25., |mut h| {
                    for text in [
                        tr("Topico"),
                        tr("Particao"),
                        tr("Offset"),
                        tr("Timestamp (UTC)"),
                        tr("Chave"),
                        tr("Valor"),
                    ] {
                        h.col(|ui| {
                            ui.strong(text);
                        });
                    }
                })
                .body(|body| {
                    body.rows(27., self.records.len(), |mut row| {
                        let index = row.index();
                        let r = &self.records[index];
                        row.col(|ui| {
                            if ui
                                .selectable_label(self.selected_record == Some(index), &r.topic)
                                .clicked()
                                && !self.busy
                            {
                                select = Some(index);
                            }
                        });
                        row.col(|ui| {
                            ui.label(r.partition.to_string());
                        });
                        row.col(|ui| {
                            ui.monospace(r.offset.to_string());
                        });
                        row.col(|ui| {
                            ui.label(
                                r.timestamp
                                    .map(|n| {
                                        chrono::DateTime::from_timestamp_millis(n)
                                            .map(|d| d.format("%H:%M:%S%.3f").to_string())
                                            .unwrap_or_default()
                                    })
                                    .unwrap_or_else(|| "—".into()),
                            );
                        });
                        row.col(|ui| {
                            if ui
                                .selectable_label(
                                    self.selected_record == Some(index),
                                    preview(r.key.as_deref()),
                                )
                                .clicked()
                                && !self.busy
                            {
                                select = Some(index);
                            }
                        });
                        row.col(|ui| {
                            if ui
                                .selectable_label(
                                    self.selected_record == Some(index),
                                    preview(r.value.as_deref()),
                                )
                                .clicked()
                                && !self.busy
                            {
                                select = Some(index);
                            }
                        });
                    });
                });
        });
        if let Some(index) = select {
            self.selected_record = Some(index);
            self.decode_selected(ctx);
        }
        if self.records.is_empty() {
            ui.weak(tr("Nenhuma mensagem carregada"));
            return;
        }
        ui.separator();
        ui.add_enabled_ui(!self.busy, |ui| {
            ui.horizontal_wrapped(|ui| {
                let previous = self.format;
                ui.label(tr("Valor:"));
                egui::ComboBox::from_id_salt("format")
                    .selected_text(self.format.label())
                    .show_ui(ui, |ui| {
                        for f in DisplayFormat::ALL {
                            ui.selectable_value(&mut self.format, f, f.label());
                        }
                    });
                if self.format != previous {
                    self.decode_selected(ctx);
                }
                if ui.button(tr("Editar e reenviar")).clicked() {
                    self.prepare_resend();
                }
                if ui.button(tr("Salvar mensagem")).clicked()
                    && let Some(index) = self.selected_record
                {
                    let record = self.records[index].clone();
                    self.pending_save = Some(PendingSave {
                        record,
                        path: default_save_path("message.jsonl"),
                        raw: false,
                    });
                }
                if ui.button(tr("Salvar valor binario")).clicked()
                    && let Some(index) = self.selected_record
                {
                    let record = self.records[index].clone();
                    self.pending_save = Some(PendingSave {
                        record,
                        path: default_save_path("value.bin"),
                        raw: true,
                    });
                }
            });
        });
        if let Some(index) = self.selected_record {
            let r = &self.records[index];
            ui.weak(tf!(
                "{0} / particao {1} / offset {2} / {3} bytes / {4} headers",
                r.topic,
                r.partition,
                r.offset,
                r.value.as_ref().map_or(0, Vec::len),
                r.headers.len()
            ));
        }
        egui::ScrollArea::vertical()
            .id_salt("detail")
            .show(ui, |ui| {
                ui.columns(2, |cols| {
                    cols[0].strong(tr("Chave"));
                    cols[0].add(
                        TextEdit::multiline(&mut self.key_display)
                            .code_editor()
                            .interactive(false)
                            .desired_width(f32::INFINITY)
                            .desired_rows(12),
                    );
                    cols[1].strong(tr("Valor"));
                    cols[1].add(
                        TextEdit::multiline(&mut self.value_display)
                            .code_editor()
                            .interactive(false)
                            .desired_width(f32::INFINITY)
                            .desired_rows(12),
                    );
                });
                if let Some(index) = self.selected_record {
                    egui::CollapsingHeader::new(tr("Headers / metadados")).show(ui, |ui| {
                        for h in &self.records[index].headers {
                            ui.monospace(format!("{} = {}", h.key, preview(h.value.as_deref())));
                        }
                    });
                }
            });
    }
    fn consumers_view(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        let groups = self
            .snapshot()
            .map(|s| s.groups.clone())
            .unwrap_or_default();
        ui.heading(tr("Consumer groups"));
        ui.add_enabled_ui(!self.busy, |ui| {
            ui.horizontal(|ui| {
                egui::ComboBox::from_id_salt("group")
                    .selected_text(if self.group.is_empty() {
                        tr("Selecione um grupo")
                    } else {
                        &self.group
                    })
                    .show_ui(ui, |ui| {
                        for g in &groups {
                            ui.selectable_value(
                                &mut self.group,
                                g.name.clone(),
                                tf!("{0} / {1} / {2} membros", g.name, g.state, g.members.len()),
                            );
                        }
                    });
                ui.add(TextEdit::singleline(&mut self.group).hint_text(tr("Ou informe group.id")));
                if ui.button(tr("Consultar offsets")).clicked() {
                    let backend = Backend::new(self.profile());
                    let group = self.group.clone();
                    let topics: Vec<_> = self.selected_topics.iter().cloned().collect();
                    self.start(ctx, tr("Consultando offsets"), move |_, _| {
                        Ok(JobResult::Offsets(backend.group_offsets(&group, &topics)?))
                    });
                }
            });
        });
        if let Some(group) = groups.iter().find(|g| g.name == self.group) {
            for m in &group.members {
                ui.weak(format!("{}  {}  {}", m.id, m.client_id, m.host));
            }
        }
        ui.separator();
        TableBuilder::new(ui)
            .striped(true)
            .column(Column::remainder())
            .column(Column::initial(70.))
            .column(Column::initial(120.))
            .column(Column::initial(100.))
            .column(Column::initial(100.))
            .column(Column::initial(100.))
            .header(25., |mut h| {
                for text in [
                    tr("Topico"),
                    tr("Particao"),
                    tr("Commit"),
                    tr("Inicio"),
                    tr("Fim"),
                    tr("Lag"),
                ] {
                    h.col(|ui| {
                        ui.strong(text);
                    });
                }
            })
            .body(|body| {
                body.rows(25., self.offsets.len(), |mut row| {
                    let o = &self.offsets[row.index()];
                    for text in [
                        o.topic.clone(),
                        o.partition.to_string(),
                        o.committed
                            .map(|n| n.to_string())
                            .unwrap_or_else(|| tr("Sem commit").into()),
                        o.low.to_string(),
                        o.high.to_string(),
                        o.lag.map(|n| n.to_string()).unwrap_or_else(|| "—".into()),
                    ] {
                        row.col(|ui| {
                            ui.label(text);
                        });
                    }
                })
            });
    }
    fn prepare_resend(&mut self) {
        let Some(index) = self.selected_record else {
            return;
        };
        let r = &self.records[index];
        let (key, key_mode) = editable(r.key.as_deref());
        let (value, value_mode) = editable(r.value.as_deref());
        self.composer=Composer{topic:r.topic.clone(),partition:r.partition.to_string(),key,value,key_mode,value_mode,key_null:r.key.is_none(),value_null:r.value.is_none(),
            headers:serde_json::to_string_pretty(&r.headers.iter().map(|h|serde_json::json!({"key":h.key,"value_base64":h.value.as_ref().map(|v|STANDARD.encode(v))})).collect::<Vec<_>>()).unwrap(),
            timestamp:r.timestamp.map(|n|n.to_string()).unwrap_or_default(),schema:self.profile().avro_schema,schema_id:String::new(),source:Some((r.topic.clone(),r.partition,r.offset))};
        if let Some(value) = &r.value
            && value.len() >= 5
            && value[0] == 0
            && serde_json::from_str::<serde_json::Value>(&self.value_display).is_ok()
        {
            self.composer.value = self.value_display.clone();
            self.composer.value_mode = 3;
            self.composer.schema_id =
                u32::from_be_bytes(value[1..5].try_into().unwrap()).to_string();
            self.composer.schema.clear();
        }
        self.tab = Tab::Compose;
    }
    fn composer_view(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        egui::ScrollArea::vertical().show(ui,|ui|{ui.heading(tr("Enviar mensagem"));if let Some((topic, partition, offset))=&self.composer.source{ui.label(tf!("Origem: {0} / {1} / offset {2}", topic, partition, offset));ui.weak(tr("O reenvio cria uma nova mensagem no destino."));}
            ui.add_enabled_ui(!self.busy,|ui|{
                ui.horizontal(|ui|{ui.label(tr("Topico"));ui.text_edit_singleline(&mut self.composer.topic);ui.label(tr("Particao"));ui.add(TextEdit::singleline(&mut self.composer.partition).hint_text(tr("Auto")).desired_width(80.));});
                ui.columns(2,|columns|{
                    columns[0].horizontal(|ui|{ui.strong(tr("Chave"));ui.checkbox(&mut self.composer.key_null,"null");encoding_combo(ui,"key_encoding",&mut self.composer.key_mode,false);});
                    columns[0].add_enabled(!self.composer.key_null,TextEdit::multiline(&mut self.composer.key).code_editor().desired_rows(9).desired_width(f32::INFINITY));
                    columns[1].horizontal(|ui|{ui.strong(tr("Valor"));ui.checkbox(&mut self.composer.value_null,"null");encoding_combo(ui,"value_encoding",&mut self.composer.value_mode,true);});
                    columns[1].add_enabled(!self.composer.value_null,TextEdit::multiline(&mut self.composer.value).code_editor().desired_rows(9).desired_width(f32::INFINITY));
                });
                if self.composer.value_mode==3{ui.horizontal(|ui|{ui.label(tr("Schema id (opcional)"));ui.text_edit_singleline(&mut self.composer.schema_id);});ui.add(TextEdit::multiline(&mut self.composer.schema).hint_text(tr("Schema Avro JSON, ou vazio para carregar o id do registry")).code_editor().desired_rows(5).desired_width(f32::INFINITY));}
                egui::CollapsingHeader::new(tr("Headers e timestamp")).show(ui,|ui|{ui.label(tr("Headers JSON: [{\"key\":\"nome\",\"value_base64\":\"...\"}] (aceita null e nomes repetidos)"));ui.add(TextEdit::multiline(&mut self.composer.headers).code_editor().desired_rows(4).desired_width(f32::INFINITY));ui.horizontal(|ui|{ui.label(tr("Timestamp Unix ms"));ui.add(TextEdit::singleline(&mut self.composer.timestamp).hint_text(tr("Vazio = horario do broker")));});});
                if ui.button(RichText::new(tr("Enviar ao Kafka")).strong()).clicked(){let draft=self.composer.clone();let backend=Backend::new(self.profile());self.start(ctx,tr("Aguardando confirmacao do broker"),move|_,_|{let mut decoder=Decoder::new(&backend.profile)?;let record=compose(&draft,&mut decoder)?;let(p,o)=backend.send(&record)?;Ok(JobResult::Published(record.topic,p,o))});}
            });
        });
    }
    fn transfer_view(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        egui::ScrollArea::vertical().show(ui,|ui|{ui.add_enabled_ui(!self.busy,|ui|{
            self.read_controls(ui);ui.separator();
            ui.horizontal(|ui|{ui.label(tr("Cluster de destino"));egui::ComboBox::from_id_salt("destination_cluster").selected_text(&self.profiles[self.destination_cluster.min(self.profiles.len()-1)].name).show_ui(ui,|ui|{for(index,p)in self.profiles.iter().enumerate(){ui.selectable_value(&mut self.destination_cluster,index,&p.name);}});ui.label(tr("Topico de destino"));ui.text_edit_singleline(&mut self.destination_topic);});
            ui.horizontal(|ui|{ui.checkbox(&mut self.keep_partition,tr("Preservar particao"));ui.checkbox(&mut self.keep_timestamp,tr("Preservar timestamp"));});ui.weak(tr("Chave, valor e todos os headers sao preservados. Offsets novos sao atribuidos pelo destino."));
            ui.group(|ui|{ui.heading(tr("Exportar topicos para arquivo"));ui.horizontal(|ui|{ui.add(TextEdit::singleline(&mut self.export_path).hint_text(tr("Arquivo .jsonl")).desired_width(420.));if ui.button(tr("Escolher arquivo")).clicked()&& let Some(path)=rfd::FileDialog::new().set_file_name("messages.jsonl").save_file(){self.export_path=path.display().to_string();}});
                if ui.button(tr("Exportar faixa selecionada")).clicked(){match self.options(){Ok(options)=>{let backend=Backend::new(self.profile());let path=PathBuf::from(&self.export_path);self.start(ctx,tr("Exportando mensagens"),move|cancel,progress|Ok(JobResult::Report(backend.export(&options,&path,&cancel,progress)?)));},Err(e)=>self.error=Some(e.to_string())}}
                if ui.button(tr("Exportar mensagens da tabela")).clicked(){let records=self.records.clone();let path=PathBuf::from(&self.export_path);self.start(ctx,tr("Salvando tabela"),move|_,_|{ensure!(!records.is_empty(),tr("A tabela esta vazia"));storage::save_records(&path,&records)?;Ok(JobResult::Saved(path.display().to_string()))});}
            });
            ui.group(|ui|{ui.heading(tr("Importar arquivo para topico"));ui.label(tr("O arquivo JSONL exportado preserva binarios, tombstones e headers."));ui.horizontal(|ui|{ui.add(TextEdit::singleline(&mut self.import_path).hint_text(tr("Arquivo .jsonl")).desired_width(420.));if ui.button(tr("Abrir arquivo")).clicked()&& let Some(path)=rfd::FileDialog::new().pick_file(){self.import_path=path.display().to_string();}});
                if ui.button(tr("Validar e importar")).clicked(){let backend=Backend::new(self.profiles[self.destination_cluster].clone());let path=PathBuf::from(&self.import_path);let topic=self.destination_topic.clone();let keep_partition=self.keep_partition;let keep_timestamp=self.keep_timestamp;self.start(ctx,tr("Validando arquivo de importacao"),move|cancel,progress|Ok(JobResult::Report(backend.import(&path,&topic,keep_partition,keep_timestamp,&cancel,progress)?)));}
            });
            ui.group(|ui|{ui.heading(tr("Replay para outro topico"));ui.label(tr("Le o snapshot da origem e aguarda a confirmacao de cada envio."));if ui.button(tr("Executar replay")).clicked(){match self.options(){Ok(options)=>{let source=Backend::new(self.profile());let destination=Backend::new(self.profiles[self.destination_cluster].clone());let replay=ReplayOptions{destination_topic:self.destination_topic.clone(),preserve_partition:self.keep_partition,preserve_timestamp:self.keep_timestamp};self.start(ctx,tr("Executando replay"),move|cancel,progress|Ok(JobResult::Report(source.replay(&options,&destination,&replay,&cancel,progress)?)));},Err(e)=>self.error=Some(e.to_string())}}});
        });});
    }
    fn search_view(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        ui.heading(tr("Buscar em topicos e particoes"));
        ui.add_enabled_ui(!self.busy, |ui| {
            self.read_controls(ui);
            ui.add(
                TextEdit::singleline(&mut self.search.pattern)
                    .hint_text(tr("Texto ou expressao regular"))
                    .desired_width(f32::INFINITY),
            );
            ui.horizontal_wrapped(|ui| {
                ui.checkbox(&mut self.search.regex, tr("Regex"));
                ui.checkbox(
                    &mut self.search.case_sensitive,
                    tr("Diferenciar maiusculas"),
                );
                ui.checkbox(&mut self.search.keys, tr("Chaves"));
                ui.checkbox(&mut self.search.values, tr("Valores"));
                ui.checkbox(&mut self.search.headers, tr("Headers"));
                if ui.button(tr("Buscar no Kafka")).clicked() {
                    self.load_messages(ctx, true);
                }
            });
        });
        ui.label(tf!(
            "{0} resultados. Abra a aba Mensagens para inspecionar, salvar ou reenviar.",
            self.records.len()
        ));
        if ui.button(tr("Ver resultados")).clicked() {
            self.tab = Tab::Messages;
        }
    }
    fn save_window(&mut self, ctx: &egui::Context) {
        let Some(mut pending) = self.pending_save.take() else {
            return;
        };
        if self.busy {
            self.pending_save = Some(pending);
            return;
        }
        let mut open = true;
        let mut save = false;
        let mut cancel = false;
        egui::Window::new(if pending.raw {
            tr("Salvar valor binario")
        } else {
            tr("Salvar mensagem")
        })
        .open(&mut open)
        .default_width(620.)
        .show(ctx, |ui| {
            ui.label(tf!(
                "{0} / particao {1} / offset {2}",
                pending.record.topic,
                pending.record.partition,
                pending.record.offset
            ));
            ui.label(tr("Arquivo de destino"));
            ui.add(TextEdit::singleline(&mut pending.path).desired_width(f32::INFINITY));
            if ui.button(tr("Escolher local...")).clicked()
                && let Some(path) = rfd::FileDialog::new()
                    .set_file_name(if pending.raw {
                        "value.bin"
                    } else {
                        "message.jsonl"
                    })
                    .save_file()
            {
                pending.path = path.display().to_string();
            }
            ui.horizontal(|ui| {
                save = ui.button(tr("Salvar")).clicked();
                cancel = ui.button(tr("Cancelar")).clicked();
            });
        });
        if save {
            let path = PathBuf::from(&pending.path);
            let record = pending.record.clone();
            let raw = pending.raw;
            self.start(ctx, tr("Salvando mensagem"), move |_, _| {
                if raw {
                    storage::save_raw(&path, record.value.as_deref())?;
                } else {
                    storage::save_records(&path, &[record])?;
                }
                Ok(JobResult::Saved(path.display().to_string()))
            });
        } else if open && !cancel {
            self.pending_save = Some(pending);
        }
    }
    fn profile_window(&mut self, ctx: &egui::Context) {
        let Some(mut p) = self.profile_edit.take() else {
            return;
        };
        if self.busy {
            self.profile_edit = Some(p);
            return;
        }
        let mut open = true;
        let mut save = false;
        let mut cancel = false;
        egui::Window::new(tr("Configuracao do cluster")).open(&mut open).default_width(620.).resizable(true).show(ctx,|ui|{egui::ScrollArea::vertical().max_height(650.).show(ui,|ui|{
            field(ui,tr("Nome"),&mut p.name,false);field(ui,tr("Bootstrap servers"),&mut p.bootstrap_servers,false);
            egui::ComboBox::from_id_salt("security").selected_text(&p.security_protocol).show_ui(ui,|ui|{for protocol in["PLAINTEXT","SSL","SASL_PLAINTEXT","SASL_SSL"]{ui.selectable_value(&mut p.security_protocol,protocol.into(),protocol);}});
            if p.security_protocol.starts_with("SASL"){egui::ComboBox::from_id_salt("mechanism").selected_text(&p.sasl_mechanism).show_ui(ui,|ui|{for mechanism in["PLAIN","SCRAM-SHA-256","SCRAM-SHA-512"]{ui.selectable_value(&mut p.sasl_mechanism,mechanism.into(),mechanism);}});field(ui,tr("Usuario"),&mut p.username,false);field(ui,tr("Senha (sessao)"),&mut p.password,true);field(ui,tr("Variavel de ambiente da senha"),&mut p.password_env,false);}
            if p.security_protocol.ends_with("SSL"){field(ui,tr("CA PEM (opcional)"),&mut p.ca_path,false);field(ui,tr("Certificado cliente PEM"),&mut p.certificate_path,false);field(ui,tr("Chave cliente PEM"),&mut p.private_key_path,false);}
            ui.separator();ui.strong(tr("Avro / Schema Registry"));field(ui,tr("URL do registry"),&mut p.registry_url,false);field(ui,tr("Usuario do registry"),&mut p.registry_username,false);field(ui,tr("Senha do registry (sessao)"),&mut p.registry_password,true);field(ui,tr("Variavel da senha do registry"),&mut p.registry_password_env,false);ui.add(TextEdit::multiline(&mut p.avro_schema).hint_text(tr("Schema Avro local (opcional)")).code_editor().desired_rows(4).desired_width(f32::INFINITY));
            egui::CollapsingHeader::new(tr("Propriedades Kafka adicionais (JSON)")).show(ui,|ui|{ui.add(TextEdit::multiline(&mut self.properties_json).code_editor().desired_rows(4).desired_width(f32::INFINITY));});
            ui.weak(tr("Senhas digitadas ficam apenas na sessao; use variaveis de ambiente para reaproveitar credenciais."));ui.horizontal(|ui|{save=ui.button(tr("Salvar cluster")).clicked();cancel=ui.button(tr("Cancelar")).clicked();});
        });});
        if save {
            match serde_json::from_str::<BTreeMap<String, String>>(&self.properties_json) {
                Ok(properties) => {
                    p.properties = properties;
                    let mut profiles = self.profiles.clone();
                    if let Some(index) = profiles.iter().position(|v| v.id == p.id) {
                        profiles[index] = p.clone();
                    } else {
                        profiles.push(p.clone());
                    }
                    match storage::save_profiles(&self.config_path, &profiles) {
                        Ok(()) => {
                            self.profiles = profiles;
                            self.selected_cluster =
                                self.profiles.iter().position(|v| v.id == p.id).unwrap();
                            self.snapshots.remove(&p.id);
                            self.selected_topics.clear();
                            self.records.clear();
                            self.offsets.clear();
                            self.selected_record = None;
                            self.key_display.clear();
                            self.value_display.clear();
                            self.group.clear();
                            self.composer = Composer {
                                headers: "[]".into(),
                                key_null: true,
                                ..Default::default()
                            };
                            self.log(tr("Cluster salvo").into());
                            open = false;
                        }
                        Err(e) => self.error = Some(format!("{e:#}")),
                    }
                }
                Err(e) => self.error = Some(tf!("Propriedades invalidas: {0}", e)),
            }
        }
        if open && !cancel {
            self.profile_edit = Some(p);
        }
    }
    fn smoke(&mut self, ctx: &egui::Context) {
        if self.smoke_output.is_none() {
            return;
        }
        if !self.smoke_started {
            self.smoke_started = true;
            self.connect_all(ctx);
        }
        if !self.busy && self.records.is_empty() && self.snapshot().is_some() {
            let report_path = PathBuf::from(format!(
                "artifacts/integration-{}.json",
                std::env::consts::OS
            ));
            // Optional settings for the native GUI benchmark; normal sessions
            // and the existing integration smoke keep their usual behavior.
            let preferred = std::env::var("KAFUNEKAFKA_SMOKE_TOPIC").ok().or_else(|| {
                std::fs::read(report_path)
                    .ok()
                    .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
                    .and_then(|v| v["source_topic"].as_str().map(str::to_owned))
            });
            let topic = preferred
                .filter(|name| {
                    self.snapshot()
                        .unwrap()
                        .topics
                        .iter()
                        .any(|t| &t.name == name)
                })
                .or_else(|| {
                    self.snapshot()
                        .unwrap()
                        .topics
                        .iter()
                        .find(|t| t.name.starts_with("kafunekafka-it-source"))
                        .map(|t| t.name.clone())
                });
            if let Some(topic) = topic {
                self.topic_filter = topic.clone();
                self.selected_topics.insert(topic);
                self.tab = Tab::Messages;
                self.start_mode = 0;
                self.limit = std::env::var("KAFUNEKAFKA_SMOKE_LIMIT")
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                    .filter(|n| *n > 0 && *n <= 20_000)
                    .unwrap_or(self.limit);
                self.smoke_read_started = Some(Instant::now());
                self.load_messages(ctx, false);
            }
        }
        if !self.busy
            && !self.records.is_empty()
            && !self.value_display.is_empty()
            && !self.screenshot_requested
        {
            self.screenshot_requested = true;
            println!(
                "UI_BENCH {}",
                serde_json::json!({
                    "phase":"data_ready", "trial":self.smoke_trial, "records":self.records.len(),
                    "elapsed_seconds":self.smoke_read_started.map(|s| s.elapsed().as_secs_f64()),
                    "scope":"read dispatch to UI data, selected-message formatting and table layout; excludes connection and screenshot readback"
                })
            );
            ctx.send_viewport_cmd(egui::ViewportCommand::Screenshot(Default::default()));
        }
        let events = ctx.input(|i| i.events.clone());
        for event in events {
            if let egui::Event::Screenshot { image, .. } = event {
                println!(
                    "UI_BENCH {}",
                    serde_json::json!({
                        "phase":"frame_captured", "trial":self.smoke_trial, "records":self.records.len(),
                        "elapsed_seconds":self.smoke_read_started.map(|s| s.elapsed().as_secs_f64()),
                        "scope":"read dispatch through rendered-frame readback; includes screenshot overhead"
                    })
                );
                let path = self.smoke_output.as_ref().unwrap();
                if let Some(parent) = path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let pixels: Vec<u8> = image.pixels.iter().flat_map(|p| p.to_array()).collect();
                match image::save_buffer(
                    path,
                    &pixels,
                    image.size[0] as u32,
                    image.size[1] as u32,
                    image::ColorType::Rgba8,
                ) {
                    Ok(()) => {
                        println!("UI_SMOKE_OK {}", path.display());
                        self.smoke_remaining -= 1;
                        if self.smoke_remaining == 0 {
                            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                        } else {
                            self.smoke_trial += 1;
                            self.records.clear();
                            self.value_display.clear();
                            self.key_display.clear();
                            self.selected_record = None;
                            self.screenshot_requested = false;
                            self.smoke_read_started = Some(Instant::now());
                            self.load_messages(ctx, false);
                        }
                    }
                    Err(e) => {
                        eprintln!("UI_SMOKE_FAILED {e}");
                        ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                    }
                }
            }
        }
        if Instant::now() > self.smoke_deadline {
            eprintln!("UI_SMOKE_FAILED timeout / {}", self.status);
            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
        }
        ctx.request_repaint_after(Duration::from_millis(100));
    }
}
impl Drop for KafuneKafkaApp {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
    }
}
impl eframe::App for KafuneKafkaApp {
    fn update(&mut self, ctx: &egui::Context, _: &mut eframe::Frame) {
        self.language.with(|| self.render(ctx));
    }
}
impl KafuneKafkaApp {
    fn change_language(&mut self, language: Language) {
        self.language = language;
        language.with(|| {
            match storage::save_settings(&self.settings_path, &UiSettings { language }) {
                Ok(()) => self.log(tr("Idioma alterado").into()),
                Err(error) => {
                    self.error = Some(tf!("Nao foi possivel salvar o idioma: {0}", error))
                }
            }
        });
    }

    fn render(&mut self, ctx: &egui::Context) {
        self.receive();
        egui::TopBottomPanel::top("brand").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading(RichText::new("kafunekafka").color(Color32::from_rgb(85, 180, 255)));
                ui.weak(tr("Explorar • inspecionar • reproduzir"));
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    let mut language = self.language;
                    ui.add_enabled_ui(!self.busy, |ui| {
                        egui::ComboBox::from_id_salt("language")
                            .selected_text(language.name())
                            .width(110.)
                            .show_ui(ui, |ui| {
                                for option in Language::ALL {
                                    ui.selectable_value(&mut language, option, option.name());
                                }
                            });
                        ui.label(tr("Idioma"));
                    });
                    if language != self.language {
                        self.change_language(language);
                        if self.selected_record.is_some() {
                            language.with(|| self.decode_selected(ctx));
                        }
                        ctx.request_repaint();
                    }
                    ui.label(&self.profile().name);
                });
            });
        });
        egui::TopBottomPanel::bottom("status").show(ctx, |ui| {
            if self.busy {
                ui.horizontal_wrapped(|ui| {
                    ui.spinner();
                    ui.label(&self.progress.phase);
                    ui.label(tf!(
                        "{0} examinadas / {1} resultados / {2} confirmadas",
                        self.progress.scanned,
                        self.progress.matched,
                        self.progress.delivered
                    ));
                    if ui.button(tr("Cancelar")).clicked() {
                        self.cancel.store(true, Ordering::Relaxed);
                    }
                });
            } else {
                ui.label(&self.status);
            }
            if let Some(error) = &self.error {
                ui.colored_label(Color32::from_rgb(255, 125, 115), error);
            }
            egui::CollapsingHeader::new(tr("Historico de operacoes")).show(ui, |ui| {
                egui::ScrollArea::vertical()
                    .max_height(130.)
                    .show(ui, |ui| {
                        for line in self.logs.iter().rev() {
                            ui.monospace(line);
                        }
                    });
            });
        });
        self.sidebar(ctx);
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.horizontal_wrapped(|ui| {
                for tab in [
                    Tab::Cluster,
                    Tab::Messages,
                    Tab::Consumers,
                    Tab::Compose,
                    Tab::Transfer,
                    Tab::Search,
                ] {
                    ui.selectable_value(&mut self.tab, tab, tab.label());
                }
            });
            ui.separator();
            match self.tab {
                Tab::Cluster => self.cluster_view(ui),
                Tab::Messages => self.messages_view(ui, ctx),
                Tab::Consumers => self.consumers_view(ui, ctx),
                Tab::Compose => self.composer_view(ui, ctx),
                Tab::Transfer => self.transfer_view(ui, ctx),
                Tab::Search => self.search_view(ui, ctx),
            }
        });
        self.profile_window(ctx);
        self.save_window(ctx);
        self.smoke(ctx);
        if self.busy {
            ctx.request_repaint_after(Duration::from_millis(100));
        }
    }
}
fn default_save_path(name: &str) -> String {
    let directory = directories::UserDirs::new()
        .map(|d| d.home_dir().to_owned())
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_default());
    directory.join(name).display().to_string()
}
fn field(ui: &mut egui::Ui, label: &str, value: &mut String, password: bool) {
    ui.horizontal(|ui| {
        ui.label(label);
        ui.add(
            TextEdit::singleline(value)
                .password(password)
                .desired_width(360.),
        );
    });
}
fn decode_record(
    decoder: &mut Decoder,
    record: &KafkaRecord,
    format: DisplayFormat,
) -> (String, String) {
    let mut display = |bytes: Option<&[u8]>, format| {
        decoder.display(bytes, format).unwrap_or_else(|e| {
            tf!(
                "Erro de decodificacao: {0}\n\nBytes (base64): {1}",
                format!("{:#}", e),
                bytes.map(|v| STANDARD.encode(v)).unwrap_or_default()
            )
        })
    };
    (
        display(record.key.as_deref(), DisplayFormat::Auto),
        display(record.value.as_deref(), format),
    )
}
fn preview(bytes: Option<&[u8]>) -> String {
    match bytes {
        None => "null".into(),
        Some([]) => tr("(vazio)").into(),
        Some(bytes)
            if std::str::from_utf8(&bytes[..bytes.len().min(90)])
                .is_err_and(|e| e.error_len().is_some())
                || bytes
                    .iter()
                    .take(90)
                    .any(|b| *b < 32 && !b"\r\n\t".contains(b)) =>
        {
            tf!(
                "[binario, {0} B] {1}",
                bytes.len(),
                hex::encode(&bytes[..bytes.len().min(14)])
            )
        }
        Some(bytes) => {
            let text =
                String::from_utf8_lossy(&bytes[..bytes.len().min(90)]).replace(['\n', '\r'], " ");
            if bytes.len() > 90 {
                format!("{text}…")
            } else {
                text
            }
        }
    }
}
fn editable(bytes: Option<&[u8]>) -> (String, usize) {
    match bytes {
        None => (String::new(), 0),
        Some(bytes) => match std::str::from_utf8(bytes) {
            Ok(text) => (text.into(), 0),
            Err(_) => (STANDARD.encode(bytes), 1),
        },
    }
}
fn encoding_combo(ui: &mut egui::Ui, id: &str, value: &mut usize, avro: bool) {
    egui::ComboBox::from_id_salt(id)
        .selected_text([tr("Texto UTF-8"), "Base64", "Hex", tr("Avro JSON")][*value])
        .show_ui(ui, |ui| {
            for (index, label) in [tr("Texto UTF-8"), "Base64", "Hex", tr("Avro JSON")]
                .iter()
                .enumerate()
                .take(if avro { 4 } else { 3 })
            {
                ui.selectable_value(value, index, *label);
            }
        });
}
fn encode(text: &str, mode: usize) -> Result<Vec<u8>> {
    match mode {
        0 => Ok(text.as_bytes().to_vec()),
        1 => Ok(STANDARD
            .decode(text.trim())
            .context(tr("Base64 invalido"))?),
        2 => Ok(hex::decode(text.trim()).context(tr("Hex invalido"))?),
        _ => anyhow::bail!(tr("Codificacao invalida")),
    }
}
fn compose(draft: &Composer, decoder: &mut Decoder) -> Result<ProduceRecord> {
    let partition = if draft.partition.trim().is_empty() {
        None
    } else {
        Some(
            draft
                .partition
                .parse::<i32>()
                .context(tr("Particao invalida"))?,
        )
    };
    let timestamp = if draft.timestamp.trim().is_empty() {
        None
    } else {
        Some(
            draft
                .timestamp
                .parse::<i64>()
                .context(tr("Timestamp invalido"))?,
        )
    };
    let key = if draft.key_null {
        None
    } else {
        Some(encode(&draft.key, draft.key_mode)?)
    };
    let value = if draft.value_null {
        None
    } else if draft.value_mode == 3 {
        let id = if draft.schema_id.trim().is_empty() {
            None
        } else {
            Some(draft.schema_id.parse().context(tr("Schema id invalido"))?)
        };
        Some(decoder.encode_avro(&draft.value, &draft.schema, id)?)
    } else {
        Some(encode(&draft.value, draft.value_mode)?)
    };
    let json: Vec<serde_json::Value> =
        serde_json::from_str(&draft.headers).context(tr("Headers JSON invalidos"))?;
    let headers = json
        .into_iter()
        .map(|h| {
            Ok(RecordHeader {
                key: h
                    .get("key")
                    .and_then(|v| v.as_str())
                    .context(tr("Header sem key"))?
                    .into(),
                value: match h.get("value_base64") {
                    None | Some(serde_json::Value::Null) => None,
                    Some(v) => Some(
                        STANDARD.decode(
                            v.as_str()
                                .context(tr("Header value_base64 deve ser string ou null"))?,
                        )?,
                    ),
                },
            })
        })
        .collect::<Result<_>>()?;
    Ok(ProduceRecord {
        topic: draft.topic.clone(),
        partition,
        timestamp,
        key,
        value,
        headers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn language_menu_switches_and_persists_from_pointer_input() {
        fn frame(
            app: &mut KafuneKafkaApp,
            ctx: &egui::Context,
            events: Vec<egui::Event>,
        ) -> egui::FullOutput {
            let input = egui::RawInput {
                screen_rect: Some(egui::Rect::from_min_size(
                    egui::Pos2::ZERO,
                    egui::vec2(1380., 900.),
                )),
                events,
                ..Default::default()
            };
            ctx.run(input, |ctx| app.language.with(|| app.render(ctx)))
        }
        fn position(output: &egui::FullOutput, label: &str) -> egui::Pos2 {
            output
                .shapes
                .iter()
                .find_map(|shape| match &shape.shape {
                    egui::Shape::Text(text) if text.galley.job.text == label => {
                        Some(text.pos + text.galley.size() * 0.5)
                    }
                    _ => None,
                })
                .unwrap_or_else(|| panic!("Label not rendered: {label}"))
        }
        fn click(app: &mut KafuneKafkaApp, ctx: &egui::Context, position: egui::Pos2) {
            let _ = frame(
                app,
                ctx,
                vec![
                    egui::Event::PointerMoved(position),
                    egui::Event::PointerButton {
                        pos: position,
                        button: egui::PointerButton::Primary,
                        pressed: true,
                        modifiers: egui::Modifiers::NONE,
                    },
                    egui::Event::PointerButton {
                        pos: position,
                        button: egui::PointerButton::Primary,
                        pressed: false,
                        modifiers: egui::Modifiers::NONE,
                    },
                ],
            );
        }
        let directory = tempfile::tempdir().unwrap();
        let ctx = egui::Context::default();
        let mut app = KafuneKafkaApp::create(None);
        app.language = Language::Portuguese;
        app.settings_path = directory.path().join("settings.json");
        for language in [Language::English, Language::Russian] {
            let output = frame(&mut app, &ctx, vec![]);
            let selected = position(&output, app.language.name());
            click(&mut app, &ctx, selected);
            let output = frame(&mut app, &ctx, vec![]);
            let target = position(&output, language.name());
            click(&mut app, &ctx, target);
            assert_eq!(app.language, language);
            assert_eq!(
                storage::load_settings(&app.settings_path).unwrap().language,
                language
            );
            let output = frame(&mut app, &ctx, vec![]);
            let _ = position(&output, language.text("Mensagens"));
        }
    }

    #[test]
    fn selected_language_is_saved_without_changing_cluster_profiles() {
        let directory = tempfile::tempdir().unwrap();
        let profiles_path = directory.path().join("clusters.json");
        storage::save_profiles(&profiles_path, &[ClusterProfile::default()]).unwrap();
        let original = std::fs::read(&profiles_path).unwrap();
        let mut app = KafuneKafkaApp::create(None);
        app.settings_path = directory.path().join("settings.json");
        assert_eq!(
            storage::load_settings(&app.settings_path).unwrap().language,
            Language::Portuguese
        );
        for language in [Language::English, Language::Russian, Language::Portuguese] {
            app.change_language(language);
            assert_eq!(app.language, language);
            assert_eq!(app.status, language.text("Idioma alterado"));
            assert_eq!(
                storage::load_settings(&app.settings_path).unwrap().language,
                language
            );
            assert_eq!(std::fs::read(&profiles_path).unwrap(), original);
        }
        std::fs::write(&app.settings_path, r#"{"language":"unknown"}"#).unwrap();
        assert!(storage::load_settings(&app.settings_path).is_err());
    }

    #[test]
    fn worker_uses_each_jobs_language_for_progress_and_validation() {
        let (jobs, receiver) = mpsc::channel();
        let (events, replies) = mpsc::channel();
        let worker = std::thread::spawn(move || job_worker(receiver, events));
        for language in [Language::English, Language::Russian, Language::Portuguese] {
            jobs.send(Job {
                language,
                work: Box::new(|_, progress| {
                    progress(Progress {
                        phase: tr("Lendo mensagens").into(),
                        ..Default::default()
                    });
                    let backend = Backend::new(ClusterProfile::default());
                    backend.config()?;
                    anyhow::bail!(tr("Selecione topicos na barra lateral"))
                }),
                cancel: cancellation(),
                ctx: egui::Context::default(),
                trace: crate::perf::Trace::new("locale_test"),
            })
            .unwrap();
            match replies.recv_timeout(Duration::from_secs(5)).unwrap() {
                Event::Progress(progress) => {
                    assert_eq!(progress.phase, language.text("Lendo mensagens"))
                }
                _ => panic!("Expected localized progress"),
            }
            match replies.recv_timeout(Duration::from_secs(5)).unwrap() {
                Event::Done(Err(error), _) => {
                    assert_eq!(error, language.text("Selecione topicos na barra lateral"))
                }
                _ => panic!("Expected localized validation error"),
            }
        }
        drop(jobs);
        worker.join().unwrap();
    }

    #[test]
    fn worker_reuses_thread_and_delivers_next_job_after_a_panic() {
        let (jobs, receiver) = mpsc::channel();
        let (events, replies) = mpsc::channel();
        let worker = std::thread::spawn(move || job_worker(receiver, events));
        for index in 0..3 {
            jobs.send(Job {
                language: Language::Portuguese,
                work: Box::new(move |_, _| {
                    assert_ne!(index, 1, "fixture panic");
                    Ok(JobResult::Saved(format!(
                        "{:?}",
                        std::thread::current().id()
                    )))
                }),
                cancel: cancellation(),
                ctx: egui::Context::default(),
                trace: crate::perf::Trace::new("worker_test"),
            })
            .unwrap();
        }
        let mut threads = vec![];
        for index in 0..3 {
            match replies.recv_timeout(Duration::from_secs(5)).unwrap() {
                Event::Done(Ok(JobResult::Saved(thread)), _) => threads.push(thread),
                Event::Done(Err(error), _) => {
                    assert_eq!(index, 1);
                    assert!(error.contains("inesperadamente"));
                }
                _ => panic!("unexpected worker result"),
            }
        }
        assert_eq!(threads.len(), 2);
        assert_eq!(threads[0], threads[1]);
        drop(jobs);
        worker.join().unwrap();
    }
    #[test]
    fn resend_draft_preserves_binary_tombstone_and_headers_then_allows_edits() {
        let r = KafkaRecord {
            topic: "source".into(),
            partition: 2,
            offset: 99,
            timestamp: Some(123),
            key: Some(vec![0, 255]),
            value: None,
            headers: vec![RecordHeader {
                key: "x".into(),
                value: Some(vec![]),
            }],
        };
        let mut app = KafuneKafkaApp::create(None);
        app.records = vec![r.clone()];
        app.selected_record = Some(0);
        app.prepare_resend();
        let mut decoder = Decoder::new(&app.profile()).unwrap();
        let recreated = compose(&app.composer, &mut decoder).unwrap();
        assert_eq!(recreated.key, r.key);
        assert_eq!(recreated.value, r.value);
        assert_eq!(recreated.headers, r.headers);
        app.composer.value_null = false;
        app.composer.value = "edited".into();
        assert_eq!(
            compose(&app.composer, &mut decoder).unwrap().value,
            Some(b"edited".to_vec())
        );
    }
}
