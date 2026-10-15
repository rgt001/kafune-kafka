//! Reproducible benchmark driver. Operations use the same Backend as the desktop UI.
use anyhow::{Context, Result, ensure};
use kafka_lens::{
    backend::{Backend, cancellation},
    model::*,
    storage,
};
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::{
    io::{BufWriter, Write},
    path::Path,
    time::Instant,
};

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let operation = args
        .get(1)
        .context("operation: fixture|snapshot|read|view|search|export|import|replay|archive")?;
    let topic = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "kafkalens-perf-source".into());
    let count: usize = args
        .get(3)
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(10_000);
    let path = args.get(4).map(String::as_str).unwrap_or("benchmark.jsonl");
    let destination = args
        .get(5)
        .map(String::as_str)
        .unwrap_or("kafkalens-perf-destination");
    let profile = ClusterProfile {
        bootstrap_servers: std::env::var("KAFKALENS_BROKERS")
            .unwrap_or_else(|_| "localhost:19092".into()),
        ..Default::default()
    };
    let backend = Backend::new(profile);
    let options = ReadOptions {
        topics: vec![topic.clone()],
        start: StartPosition::Earliest,
        max_messages: count,
        max_scanned: count,
        timeout_seconds: 300,
        ..Default::default()
    };
    let started = Instant::now();
    let mut report = match operation.as_str() {
        "fixture" => {
            let partitions = args
                .get(6)
                .map(|s| s.parse::<i32>())
                .transpose()?
                .unwrap_or(3);
            backend.create_fixture_topic(&topic, partitions)?;
            backend.create_fixture_topic(destination, partitions)?;
            let producer: FutureProducer = backend
                .config()?
                .set("enable.idempotence", "true")
                .set("acks", "all")
                .set("linger.ms", "5")
                .create()?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let mut pending = Vec::new();
            let mut archive = storage::AtomicArchive::new(Path::new(path))?;
            for n in 0..count {
                let value = serde_json::to_vec(
                    &serde_json::json!({"id": n, "status": if n%100==0 {"target"} else {"normal"}, "body": "x".repeat(900)}),
                )?;
                let record = KafkaRecord {
                    topic: topic.clone(),
                    partition: (n % partitions as usize) as i32,
                    offset: (n / partitions as usize) as i64,
                    timestamp: Some(1791936000000),
                    key: Some(format!("key-{n:08}").into_bytes()),
                    value: Some(value),
                    headers: vec![],
                };
                archive.write(&record)?;
                pending.push(
                    producer
                        .send_result(
                            FutureRecord::<[u8], [u8]>::to(&topic)
                                .partition(record.partition)
                                .key(record.key.as_deref().unwrap())
                                .payload(record.value.as_deref().unwrap())
                                .timestamp(record.timestamp.unwrap()),
                        )
                        .map_err(|(e, _)| e)?,
                );
                if pending.len() == 256 {
                    for f in pending.drain(..) {
                        runtime.block_on(f)?.map_err(|(e, _)| e)?;
                    }
                }
            }
            for f in pending {
                runtime.block_on(f)?.map_err(|(e, _)| e)?;
            }
            archive.finish()?;
            OperationReport {
                delivered: count,
                complete: true,
                ..Default::default()
            }
        }
        "snapshot" => {
            let s = backend.snapshot()?;
            OperationReport {
                matched: s.topics.len(),
                complete: true,
                ..Default::default()
            }
        }
        "read" => backend.read(&options, &cancellation(), &|_| {})?.report,
        "view" => {
            let mut output = BufWriter::new(std::io::stdout().lock());
            let r = backend.scan(&options, &cancellation(), &|_| {}, |r| {
                writeln!(
                    output,
                    "{}:{} {} {}",
                    r.partition,
                    r.offset,
                    String::from_utf8_lossy(r.key.as_deref().unwrap_or_default()),
                    String::from_utf8_lossy(r.value.as_deref().unwrap_or_default())
                )?;
                Ok(true)
            })?;
            output.flush()?;
            r
        }
        "search" | "search-view" => {
            let result = backend.search(
                &options,
                &SearchOptions {
                    pattern: "target".into(),
                    regex: true,
                    keys: false,
                    case_sensitive: true,
                    ..Default::default()
                },
                &cancellation(),
                &|_| {},
            )?;
            if operation == "search-view" {
                let mut output = BufWriter::new(std::io::stdout().lock());
                for r in &result.records {
                    writeln!(
                        output,
                        "{}:{} {} {}",
                        r.partition,
                        r.offset,
                        String::from_utf8_lossy(r.key.as_deref().unwrap_or_default()),
                        String::from_utf8_lossy(r.value.as_deref().unwrap_or_default())
                    )?;
                }
                output.flush()?;
            }
            result.report
        }
        "export" => backend.export(&options, Path::new(path), &cancellation(), &|_| {})?,
        "import" => backend.import(
            Path::new(path),
            destination,
            true,
            true,
            &cancellation(),
            &|_| {},
        )?,
        "replay" => backend.replay(
            &options,
            &backend,
            &ReplayOptions {
                destination_topic: destination.into(),
                preserve_partition: true,
                preserve_timestamp: true,
            },
            &cancellation(),
            &|_| {},
        )?,
        "archive" => {
            let mut archive = storage::AtomicArchive::new(Path::new(path))?;
            let r = KafkaRecord {
                topic,
                partition: 0,
                offset: 0,
                timestamp: Some(1791936000000),
                key: Some(b"key-00000000".to_vec()),
                value: Some(vec![b'x'; 1024]),
                headers: vec![],
            };
            for _ in 0..count {
                archive.write(&r)?;
            }
            archive.finish()?;
            OperationReport {
                matched: count,
                complete: true,
                ..Default::default()
            }
        }
        _ => anyhow::bail!("Unknown operation"),
    };
    if matches!(operation.as_str(), "read" | "view" | "export" | "replay") {
        ensure!(
            report.scanned == count,
            "Expected {count} records, got {}",
            report.scanned
        );
    }
    if operation.starts_with("search") {
        ensure!(
            report.scanned == count && report.matched == count.div_ceil(100),
            "Search count mismatch"
        );
    }
    if operation == "import" {
        ensure!(
            report.delivered == count && report.complete,
            "Import count mismatch"
        );
    }
    let elapsed = started.elapsed().as_secs_f64();
    let receipt_sample_count = report.destination_offsets.len();
    report.destination_offsets.clear();
    let output = serde_json::to_vec(
        &serde_json::json!({"operation":operation,"elapsed_seconds":elapsed,"receipt_sample_count":receipt_sample_count,"report":report}),
    )?;
    let mut stderr = std::io::stderr().lock();
    stderr.write_all(&output)?;
    stderr.write_all(b"\n")?;
    Ok(())
}
