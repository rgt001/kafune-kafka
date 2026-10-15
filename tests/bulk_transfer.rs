use anyhow::Result;
use kafka_lens::{
    backend::{Backend, cancellation},
    model::*,
    storage,
};
use std::sync::atomic::Ordering;

#[test]
#[ignore = "Requires Kafka in compose.yaml"]
fn bounded_bulk_delivery_preserves_order_and_drains_on_cancel() -> Result<()> {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = format!("kafkalens-bulk-source-{suffix}");
    let copied = format!("kafkalens-bulk-copy-{suffix}");
    let cancelled = format!("kafkalens-bulk-cancel-{suffix}");
    let rejected = format!("kafkalens-bulk-reject-{suffix}");
    let backend = Backend::new(ClusterProfile::default());
    for topic in [&source, &copied, &cancelled, &rejected] {
        backend.create_fixture_topic(topic, 3)?;
    }
    let records: Vec<_> = (0..2050)
        .map(|n| KafkaRecord {
            topic: source.clone(),
            partition: n % 3,
            offset: (n / 3) as i64,
            timestamp: Some(1791936000000),
            key: Some(n.to_be_bytes().to_vec()),
            value: if n % 101 == 0 {
                None
            } else {
                Some(vec![(n % 256) as u8; 4096])
            },
            headers: vec![
                RecordHeader {
                    key: "binary".into(),
                    value: Some(vec![0, 255]),
                },
                RecordHeader {
                    key: "binary".into(),
                    value: None,
                },
            ],
        })
        .collect();
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("bulk.jsonl");
    storage::save_records(&path, &records)?;
    let report = backend.import(&path, &source, true, true, &cancellation(), &|_| {})?;
    assert_eq!(report.delivered, records.len());
    assert!(report.complete);
    let options = ReadOptions {
        topics: vec![source.clone()],
        start: StartPosition::Earliest,
        max_messages: 3000,
        max_scanned: 3000,
        timeout_seconds: 30,
        ..Default::default()
    };
    let verify = |topic: &str, expected: usize| -> Result<()> {
        let mut options = options.clone();
        options.topics = vec![topic.into()];
        let mut actual = backend.read(&options, &cancellation(), &|_| {})?.records;
        assert_eq!(actual.len(), expected);
        actual.sort_by_key(|r| (r.partition, r.offset));
        let mut original = records.clone();
        original.sort_by_key(|r| (r.partition, r.offset));
        for (a, b) in actual.iter().zip(original.iter()) {
            assert_eq!(
                (
                    a.partition,
                    a.offset,
                    a.timestamp,
                    &a.key,
                    &a.value,
                    &a.headers
                ),
                (
                    b.partition,
                    b.offset,
                    b.timestamp,
                    &b.key,
                    &b.value,
                    &b.headers
                )
            );
        }
        Ok(())
    };
    verify(&source, records.len())?;
    let report = backend.replay(
        &options,
        &backend,
        &ReplayOptions {
            destination_topic: copied.clone(),
            preserve_partition: true,
            preserve_timestamp: true,
        },
        &cancellation(),
        &|_| {},
    )?;
    assert_eq!(report.delivered, records.len());
    verify(&copied, records.len())?;
    let short = format!("kafkalens-bulk-short-{suffix}");
    backend.create_fixture_topic(&short, 1)?;
    let replay = ReplayOptions {
        destination_topic: short.clone(),
        preserve_partition: true,
        preserve_timestamp: true,
    };
    assert!(
        backend
            .replay(&options, &backend, &replay, &cancellation(), &|_| {})
            .is_err()
    );
    let mut short_options = options.clone();
    short_options.topics = vec![short.clone()];
    assert!(
        backend
            .read(&short_options, &cancellation(), &|_| {})?
            .records
            .is_empty(),
        "Partition validation must precede every publish"
    );
    let mut one_partition = options.clone();
    one_partition.partitions = vec![0];
    let report = backend.replay(&one_partition, &backend, &replay, &cancellation(), &|_| {})?;
    let expected = records.iter().filter(|r| r.partition == 0).count();
    assert_eq!(report.delivered, expected);
    verify(&short, expected)?;
    let mut profile = ClusterProfile::default();
    profile.properties.insert("linger.ms".into(), "250".into());
    let slow = Backend::new(profile);
    let cancel = cancellation();
    let report = slow.import(&path, &cancelled, true, true, &cancel, &|p| {
        if p.scanned > 0 {
            cancel.store(true, Ordering::Relaxed);
        }
    })?;
    assert!(
        report.cancelled && report.delivered > 0 && report.delivered < records.len(),
        "{report:?}"
    );
    let mut cancelled_options = options.clone();
    cancelled_options.topics = vec![cancelled];
    let before = backend
        .read(&cancelled_options, &cancellation(), &|_| {})?
        .records;
    assert_eq!(before.len(), report.delivered);
    std::thread::sleep(std::time::Duration::from_millis(350));
    let after = backend
        .read(&cancelled_options, &cancellation(), &|_| {})?
        .records;
    assert_eq!(
        before.len(),
        after.len(),
        "Messages still arriving after cancellation returned"
    );
    for p in 0..3 {
        let rows: Vec<_> = before.iter().filter(|r| r.partition == p).collect();
        for (n, r) in rows.iter().enumerate() {
            assert_eq!(r.offset, n as i64);
            assert_eq!(r.key, Some(((n as i32) * 3 + p).to_be_bytes().to_vec()));
        }
    }
    let mut profile = ClusterProfile::default();
    profile
        .properties
        .insert("message.max.bytes".into(), "1000".into());
    let limited = Backend::new(profile);
    assert!(
        limited
            .import(&path, &rejected, true, true, &cancellation(), &|_| {})
            .is_err()
    );
    let mut rejected_options = options;
    rejected_options.topics = vec![rejected];
    // The first tombstone may have been enqueued before the oversized second
    // record failed. Any such accepted prefix must already be drained.
    let a = backend
        .read(&rejected_options, &cancellation(), &|_| {})?
        .records
        .len();
    std::thread::sleep(std::time::Duration::from_millis(350));
    let b = backend
        .read(&rejected_options, &cancellation(), &|_| {})?
        .records
        .len();
    assert_eq!(a, b);
    assert!(a <= 1);
    std::fs::create_dir_all("artifacts")?;
    std::fs::write(
        format!("artifacts/bulk-{}.json", std::env::consts::OS),
        serde_json::to_vec_pretty(
            &serde_json::json!({"status":"passed","messages":records.len(),"cancelled_delivered":report.delivered,"partition_order":"passed","binary_null_headers_timestamps":"passed","drained_cancellation_and_failure":"passed"}),
        )?,
    )?;
    Ok(())
}
