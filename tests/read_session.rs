use anyhow::Result;
use kafunekafka::{
    backend::{Backend, cancellation},
    model::*,
};
use std::sync::atomic::Ordering;

#[test]
#[ignore = "Requires the two local Kafka clusters in compose.yaml"]
fn rereads_refresh_bounds_and_recover_after_cancellation_and_errors() -> Result<()> {
    let name = format!("kafunekafka-session-{}", uuid::Uuid::new_v4().simple());
    let mut backend = Backend::new(ClusterProfile::default());
    backend.create_fixture_topic(&name, 3)?;
    let write = |backend: &Backend, partition, value: &[u8]| {
        backend.send(&ProduceRecord {
            topic: name.clone(),
            partition: Some(partition),
            timestamp: None,
            key: None,
            value: Some(value.to_vec()),
            headers: vec![],
        })
    };
    for partition in 0..3 {
        for value in [b"first".as_slice(), b"second", b"third"] {
            write(&backend, partition, value)?;
        }
    }
    let options = ReadOptions {
        topics: vec![name.clone()],
        start: StartPosition::Earliest,
        max_messages: 100,
        ..Default::default()
    };
    let read =
        |backend: &Backend, options: &ReadOptions| backend.read(options, &cancellation(), &|_| {});
    assert_eq!(read(&backend, &options)?.records.len(), 9);
    let cancel = cancellation();
    let mut visited = 0;
    let result = backend.scan(&options, &cancel, &|_| {}, |_| {
        visited += 1;
        cancel.store(true, Ordering::Relaxed);
        Ok(true)
    })?;
    assert!(result.cancelled);
    assert_eq!(visited, 1);
    let subset = ReadOptions {
        partitions: vec![1],
        start: StartPosition::Offset(1),
        ..options.clone()
    };
    let records = read(&backend, &subset)?.records;
    assert_eq!(records.len(), 2);
    assert!(records.iter().all(|r| r.partition == 1 && r.offset >= 1));
    assert_eq!(records[0].value.as_deref(), Some(b"second".as_slice()));
    // A new record must be visible even though the connection is reused.
    write(&backend, 1, b"appended")?;
    let latest = ReadOptions {
        partitions: vec![1],
        start: StartPosition::Latest(1),
        ..options.clone()
    };
    let records = read(&backend, &latest)?.records;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].value.as_deref(), Some(b"appended".as_slice()));
    assert!(
        read(
            &backend,
            &ReadOptions {
                partitions: vec![99],
                ..options.clone()
            }
        )
        .is_err()
    );
    assert_eq!(read(&backend, &options)?.records.len(), 10);
    // Change the effective connection on this very same Backend instance.
    let other = Backend::new(ClusterProfile {
        bootstrap_servers: "localhost:29092".into(),
        ..Default::default()
    });
    other.create_fixture_topic(&name, 1)?;
    write(&other, 0, b"other-cluster")?;
    backend.profile.bootstrap_servers = "localhost:29092".into();
    let records = read(&backend, &options)?.records;
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].value.as_deref(),
        Some(b"other-cluster".as_slice())
    );
    std::fs::write(
        format!("artifacts/read-session-{}.json", std::env::consts::OS),
        serde_json::to_vec_pretty(&serde_json::json!({
            "status":"passed", "fresh_watermarks":true, "cancelled_range_cleared":true,
            "partition_and_offset_changes":true, "recovery_after_error":true, "connection_change":true,
        }))?,
    )?;
    Ok(())
}
