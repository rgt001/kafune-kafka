use anyhow::Result;
use kafunekafka::{
    backend::{Backend, cancellation},
    codec::{Decoder, DisplayFormat},
    model::*,
    storage,
};
use rdkafka::{
    Offset, TopicPartitionList,
    consumer::{BaseConsumer, CommitMode, Consumer},
};
use std::{sync::atomic::Ordering, time::Duration};

#[test]
#[ignore = "Requires the two Kafka clusters and Schema Registry in compose.yaml"]
fn all_twelve_requirements_against_real_kafka() -> Result<()> {
    let language = std::env::var("KAFUNEKAFKA_LANGUAGE")
        .ok()
        .and_then(|code| kafunekafka::i18n::Language::from_code(&code))
        .unwrap_or_default();
    language.with(validate_all_twelve_requirements)
}

fn validate_all_twelve_requirements() -> Result<()> {
    let suffix = &uuid::Uuid::new_v4().simple().to_string()[..8];
    let source = format!("kafunekafka-it-source-{suffix}");
    let imported = format!("kafunekafka-it-import-{suffix}");
    let replayed = format!("kafunekafka-it-replay-{suffix}");
    let remote = format!("kafunekafka-it-remote-{suffix}");
    let p = ClusterProfile::default();
    let backend = Backend::new(p.clone());
    let secondary = Backend::new(ClusterProfile {
        bootstrap_servers: "localhost:29092".into(),
        name: "Second cluster".into(),
        ..Default::default()
    });
    for topic in [&source, &imported, &replayed] {
        backend.create_fixture_topic(topic, 3)?;
    }
    secondary.create_fixture_topic(&remote, 3)?;
    let schema = r#"{"type":"record","name":"Purchase","fields":[{"name":"customer","type":"string"},{"name":"total","type":"long"}]}"#;
    let response: serde_json::Value = reqwest::blocking::Client::new()
        .post(format!(
            "{}/subjects/kafunekafka-test-{suffix}/versions",
            p.registry_url
        ))
        .json(&serde_json::json!({"schema":schema}))
        .send()?
        .error_for_status()?
        .json()?;
    let schema_id = response["id"].as_u64().unwrap() as u32;
    let mut decoder = Decoder::new(&p)?;
    let avro = decoder.encode_avro(r#"{"customer":"Ana","total":42}"#, schema, Some(schema_id))?;
    assert!(
        decoder
            .encode_avro(
                r#"{"different":1}"#,
                r#"{"type":"record","name":"Other","fields":[{"name":"different","type":"long"}]}"#,
                Some(schema_id)
            )
            .is_err()
    );
    let buyer_schema = r#"{"type":"record","name":"Buyer","namespace":"test","fields":[{"name":"name","type":"string"}]}"#;
    let invoice_schema = r#"{"type":"record","name":"Invoice","namespace":"test","fields":[{"name":"buyer","type":"test.Buyer"}]}"#;
    let http = reqwest::blocking::Client::new();
    let buyer_subject = format!("kafunekafka-buyer-{suffix}");
    http.post(format!(
        "{}/subjects/{buyer_subject}/versions",
        p.registry_url
    ))
    .json(&serde_json::json!({"schema":buyer_schema}))
    .send()?
    .error_for_status()?;
    let invoice:serde_json::Value=http.post(format!("{}/subjects/kafunekafka-invoice-{suffix}/versions",p.registry_url)).json(&serde_json::json!({"schema":invoice_schema,"references":[{"name":"test.Buyer","subject":buyer_subject,"version":1}]})).send()?.error_for_status()?.json()?;
    let reference_payload = decoder.encode_avro(
        r#"{"buyer":{"name":"Bia"}}"#,
        "",
        Some(invoice["id"].as_u64().unwrap() as u32),
    )?;
    let reference_topic = format!("kafunekafka-it-references-{suffix}");
    backend.create_fixture_topic(&reference_topic, 1)?;
    backend.send(&ProduceRecord {
        topic: reference_topic.clone(),
        partition: Some(0),
        timestamp: None,
        key: None,
        value: Some(reference_payload),
        headers: vec![],
    })?;
    let reference_read = backend.read(
        &ReadOptions {
            topics: vec![reference_topic],
            start: StartPosition::Earliest,
            max_messages: 10,
            ..Default::default()
        },
        &cancellation(),
        &|_| {},
    )?;
    assert!(
        decoder
            .display(
                reference_read.records[0].value.as_deref(),
                DisplayFormat::Avro
            )?
            .contains("Bia")
    );
    let records = vec![
        ProduceRecord {
            topic: source.clone(),
            partition: Some(0),
            timestamp: Some(chrono::Utc::now().timestamp_millis() - 120_000),
            key: Some(b"json-key".to_vec()),
            value: Some(br#"{"kind":"order","name":"Ana","id":101}"#.to_vec()),
            headers: vec![
                RecordHeader {
                    key: "trace".into(),
                    value: Some(vec![0, 255]),
                },
                RecordHeader {
                    key: "trace".into(),
                    value: None,
                },
            ],
        },
        ProduceRecord {
            topic: source.clone(),
            partition: Some(1),
            timestamp: None,
            key: None,
            value: Some(b"<order><customer>Ana</customer></order>".to_vec()),
            headers: vec![],
        },
        ProduceRecord {
            topic: source.clone(),
            partition: Some(2),
            timestamp: None,
            key: Some(b"avro-key".to_vec()),
            value: Some(avro),
            headers: vec![],
        },
        ProduceRecord {
            topic: source.clone(),
            partition: Some(1),
            timestamp: None,
            key: Some(b"deleted".to_vec()),
            value: None,
            headers: vec![],
        },
        ProduceRecord {
            topic: source.clone(),
            partition: Some(0),
            timestamp: None,
            key: Some(vec![]),
            value: Some(vec![255, 128, 0, 10]),
            headers: vec![],
        },
        ProduceRecord {
            topic: source.clone(),
            partition: Some(2),
            timestamp: None,
            key: None,
            value: Some(vec![]),
            headers: vec![RecordHeader {
                key: "empty".into(),
                value: Some(vec![]),
            }],
        },
    ];
    for r in &records {
        let (partition, offset) = backend.send(r)?;
        assert_eq!(partition, r.partition.unwrap());
        assert!(offset >= 0);
    }
    let group = format!("kafunekafka-it-group-{suffix}");
    let mut config = backend.config()?;
    config
        .set("group.id", &group)
        .set("enable.auto.commit", "false");
    let consumer: BaseConsumer = config.create()?;
    let mut offsets = TopicPartitionList::new();
    offsets.add_partition_offset(&source, 0, Offset::Offset(1))?;
    consumer.commit(&offsets, CommitMode::Sync)?;
    let snapshot = backend.snapshot()?;
    assert!(!snapshot.brokers.is_empty());
    assert!(
        snapshot
            .topics
            .iter()
            .any(|t| t.name == source && t.partitions.len() == 3)
    );
    assert!(snapshot.groups.iter().any(|g| g.name == group));
    assert!(!secondary.snapshot()?.brokers.is_empty());
    let before = backend.group_offsets(&group, std::slice::from_ref(&source))?;
    assert!(
        before
            .iter()
            .any(|o| o.partition == 0 && o.committed == Some(1) && o.lag == Some(1))
    );
    let options = ReadOptions {
        topics: vec![source.clone()],
        start: StartPosition::Earliest,
        max_messages: 100,
        max_scanned: 100,
        timeout_seconds: 15,
        ..Default::default()
    };
    let cancel = cancellation();
    let progress = |_: Progress| {};
    let read = backend.read(&options, &cancel, &progress)?;
    assert_eq!(read.records.len(), 6);
    assert!(read.report.complete);
    for original in &records {
        assert!(
            read.records
                .iter()
                .any(|r| r.partition == original.partition.unwrap()
                    && r.key == original.key
                    && r.value == original.value
                    && r.headers == original.headers)
        );
    }
    let after = backend.group_offsets(&group, std::slice::from_ref(&source))?;
    assert_eq!(
        before.iter().map(|o| o.committed).collect::<Vec<_>>(),
        after.iter().map(|o| o.committed).collect::<Vec<_>>()
    );
    for r in &read.records {
        if let Some(value) = &r.value {
            if value.starts_with(b"{") {
                assert!(
                    decoder
                        .display(Some(value), DisplayFormat::Json)?
                        .contains("\n")
                );
            }
            if value.starts_with(b"<") {
                assert!(
                    decoder
                        .display(Some(value), DisplayFormat::Xml)?
                        .contains("\n")
                );
            }
            if value.starts_with(&[0]) {
                assert!(
                    decoder
                        .display(Some(value), DisplayFormat::Avro)?
                        .contains("\"total\": 42")
                );
            }
        }
    }
    let found = backend.search(
        &options,
        &SearchOptions {
            pattern: "Ana|101".into(),
            regex: true,
            ..Default::default()
        },
        &cancel,
        &progress,
    )?;
    assert_eq!(found.records.len(), 3);
    let dir = tempfile::tempdir()?;
    let export = dir.path().join("topic.jsonl");
    let saved = backend.export(&options, &export, &cancel, &progress)?;
    assert_eq!(saved.matched, 6);
    let single = dir.path().join("message.jsonl");
    storage::save_records(&single, &read.records[..1])?;
    let mut restored = vec![];
    storage::visit_archive(&single, |r| {
        restored.push(r);
        Ok(true)
    })?;
    assert_eq!(restored, read.records[..1]);
    let raw = dir.path().join("value.bin");
    let binary = read
        .records
        .iter()
        .find(|r| r.value == Some(vec![255, 128, 0, 10]))
        .unwrap();
    storage::save_raw(&raw, binary.value.as_deref())?;
    assert_eq!(std::fs::read(&raw)?, binary.value.clone().unwrap());
    let imported_report = backend.import(&export, &imported, true, true, &cancel, &progress)?;
    assert_eq!(imported_report.delivered, 6);
    assert!(imported_report.complete);
    let imported_read = backend.read(
        &ReadOptions {
            topics: vec![imported.clone()],
            ..options.clone()
        },
        &cancel,
        &progress,
    )?;
    assert_eq!(imported_read.records.len(), 6);
    for r in &read.records {
        assert!(
            imported_read
                .records
                .iter()
                .any(|i| i.partition == r.partition
                    && i.key == r.key
                    && i.value == r.value
                    && i.headers == r.headers
                    && i.timestamp == r.timestamp)
        );
    }
    let replay = backend.replay(
        &options,
        &backend,
        &ReplayOptions {
            destination_topic: replayed.clone(),
            preserve_partition: true,
            preserve_timestamp: true,
        },
        &cancel,
        &progress,
    )?;
    assert_eq!(replay.delivered, 6);
    assert_eq!(
        backend
            .read(
                &ReadOptions {
                    topics: vec![replayed.clone()],
                    ..options.clone()
                },
                &cancel,
                &progress
            )?
            .records
            .len(),
        6
    );
    let remote_report = backend.replay(
        &options,
        &secondary,
        &ReplayOptions {
            destination_topic: remote.clone(),
            preserve_partition: true,
            preserve_timestamp: true,
        },
        &cancel,
        &progress,
    )?;
    assert_eq!(remote_report.delivered, 6);
    let mut resend = ProduceRecord::from(&read.records[0]);
    resend.value = Some(br#"{"edited":true}"#.to_vec());
    let receipt = backend.send(&resend)?;
    let rerun = backend.read(&options, &cancel, &progress)?;
    assert_eq!(rerun.records.len(), 7);
    assert!(
        rerun
            .records
            .iter()
            .any(|r| r.partition == receipt.0 && r.offset == receipt.1 && r.value == resend.value)
    );
    assert!(
        backend
            .replay(
                &options,
                &backend,
                &ReplayOptions {
                    destination_topic: source.clone(),
                    preserve_partition: false,
                    preserve_timestamp: false
                },
                &cancel,
                &progress
            )
            .is_err()
    );
    let invalid = dir.path().join("bad.jsonl");
    std::fs::write(&invalid, "{\"not-a-record\":true}\n")?;
    assert!(
        backend
            .import(&invalid, &imported, false, false, &cancel, &progress)
            .is_err()
    );
    assert_eq!(
        backend
            .read(
                &ReadOptions {
                    topics: vec![imported],
                    ..options.clone()
                },
                &cancel,
                &progress
            )?
            .records
            .len(),
        6
    );
    let cancelled = cancellation();
    cancelled.store(true, Ordering::Relaxed);
    assert!(
        backend
            .read(&options, &cancelled, &progress)?
            .report
            .cancelled
    );
    let auth = Backend::new(ClusterProfile {
        bootstrap_servers: "localhost:19093".into(),
        security_protocol: "SASL_PLAINTEXT".into(),
        username: std::env::var("KAFUNEKAFKA_TEST_SASL_USERNAME")
            .unwrap_or_else(|_| "kafunekafka".into()),
        password: "local-test-only".into(),
        ..Default::default()
    });
    assert!(!auth.snapshot()?.brokers.is_empty());
    let multi = backend.search(
        &ReadOptions {
            topics: vec![source.clone(), replayed],
            ..options.clone()
        },
        &SearchOptions {
            pattern: "Ana".into(),
            regex: true,
            ..Default::default()
        },
        &cancel,
        &progress,
    )?;
    assert_eq!(multi.records.len(), 6);
    let report = serde_json::json!({"status":"passed","platform":std::env::consts::OS,"requirements":[1,2,3,4,5,6,7,8,9,10,11],"source_topic":source,"source_records":7,"imported_records":6,"replayed_records":6,"cross_cluster_records":6,"consumer_offsets_unchanged":true,"json_xml_avro":true,"schema_registry":true,"schema_references":true,"schema_id_mismatch_rejected":true,"sasl_plain":true,"null_binary_headers_preserved":true});
    std::fs::create_dir_all("artifacts")?;
    std::fs::write(
        format!("artifacts/integration-{}.json", std::env::consts::OS),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    // Fixtures intentionally remain available for native GUI smoke tests.
    let _ = Duration::from_secs(1);
    Ok(())
}
