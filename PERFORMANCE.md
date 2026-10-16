# kafunekafka 0.2.0 — performance measurements

Historical report from before the public rename to `kafunekafka`. Display names have been updated; results and hashes have been preserved. These figures are not new measurements of version 0.3.0.

The evidence paths below refer to the original measurement environment. Those local files are not included in this public release.

Local measurements from 2026-10-14, compared with Offset Explorer 4.0.5. The optimized version shows a time and memory advantage in the tested CLIs. JVM startup is included in these timings; the advantage cannot be applied directly to an already open interface.

## Environment and method

AMD Ryzen 7 5700G, 8 cores/16 threads, 32 GB of RAM; Windows 10 Pro 19045 and Ubuntu 22.04 on WSL2. Local Kafka 4.1.0 and Schema Registry 7.8.3. kafunekafka built in `release` mode with thin LTO; Offset Explorer installed from official distributions, with Java 21. Builds and integration tests finished before the final measurements.

Each CLI comparison uses one discarded warmup run and five measured runs per product, alternating their order. The table shows the median and minimum–maximum range in seconds. Both programs read the same 12,000 JSON messages (~940 value bytes), with keys, a fixed timestamp, and three partitions of 4,000 messages. The regex search returns the same 120 messages; SHA-256 hashes of normalized records confirm identical content in each read/search pair.

`psutil` samples the process and its children every 20 ms. Peak RSS and CPU values are sampling estimates and may miss events between samples; CPU is aggregate thread time, not a percentage. RSS for processes with shared pages does not equal unique physical memory usage. On Linux, stdout and JSONL files are on `/mnt/d` (DrvFS); that filesystem cost is included in total time. End-to-end time includes startup, connection, processing, output, and shutdown.

## CLIs — reading and searching

| Platform / operation | kafunekafka s (min–max) | Offset Explorer s (min–max) | OE/kafunekafka ratio | kafunekafka / OE RSS MiB | kafunekafka / OE CPU s |
|---|---:|---:|---:|---:|---:|
| windows / read + stdout | 0.221 (0.199–0.374) | 3.717 (3.636–3.937) | 16.8× | 15.0 / 196.4 | 0.078 / 5.047 |
| windows / regex search + stdout | 0.260 (0.199–0.322) | 3.650 (3.629–3.880) | 14.1× | 14.3 / 193.8 | 0.062 / 4.812 |
| linux / read + stdout | 1.369 (1.206–1.393) | 7.771 (7.237–8.439) | 5.7× | 26.0 / 199.2 | 0.170 / 5.480 |
| linux / regex search + stdout | 0.209 (0.173–0.210) | 3.753 (3.693–3.822) | 17.9× | 17.5 / 203.9 | 0.040 / 4.590 |

kafunekafka uses the same `Backend` as the desktop app through a development driver. The other side uses the [official Offset Explorer CLI](https://www.kafkatool.com/documentation/commandline.html). This compares two complete CLI executions, including their runtimes. The conditions are local Kafka, plaintext, and JSON; TLS, Avro, WAN latency, and production clusters were not compared for performance.

## CLIs — acknowledged replay of 12,000 messages

| Platform | kafunekafka end-to-end s (min–max) | Offset Explorer end-to-end s (min–max) | kafunekafka / OE RSS MiB | kafunekafka / OE reported time s |
|---|---:|---:|---:|---:|
| windows | 0.405 (0.362–0.511) | 2.754 (2.587–2.877) | 25.7 / 253.2 | 0.367 / 0.564 |
| linux | 0.311 (0.310–0.351) | 2.450 (2.368–2.801) | 25.8 / 234.3 | 0.295 / 0.403 |

The timers reported by the products have different boundaries: ours includes destination validation and scan setup; Offset Explorer defines its own. These are diagnostic values. In an additional run with Offset Explorer already open on X11, its interface reported **306 ms** for the same 12,000 messages. Our Linux backend took about 295 ms, but these figures do not establish a GUI latency winner. The capture is stored in `.tools/evidence/offset-warm-replay-0.png`.

## kafunekafka operations

Five runs per operation, with warmup discarded, using 10,000 messages in the original fixture. Export uses JSONL with base64 bytes and `fsync`; import validates the entire file before publishing and waits for acknowledgements. Offset Explorer's format exports individual files and is not equivalent to our JSONL; these formats are not used to claim a direct export/import advantage.

| Platform / operation | End-to-end time s (min–max) | RSS MiB |
|---|---:|---:|
| windows / snapshot | 0.199 (0.197–0.201) | 8.7 |
| windows / read | 0.200 (0.197–0.203) | 21.3 |
| windows / search | 0.200 (0.199–0.223) | 14.2 |
| windows / export | 0.241 (0.219–0.268) | 14.6 |
| windows / import | 0.767 (0.743–0.810) | 19.0 |
| linux / snapshot | 0.168 (0.168–0.169) | 10.5 |
| linux / read | 0.148 (0.148–0.189) | 25.6 |
| linux / search | 0.149 (0.147–0.188) | 20.7 |
| linux / export | 0.454 (0.330–0.962) | 23.1 |
| linux / import | 0.372 (0.371–0.426) | 19.5 |

The original import acknowledged one message at a time: the two 10,000-message runs (warmup and first measurement) have a median internal time of **155.14 s**. The final version has a median internal time of **0.741 s**, across five measurements. The number of repetitions differs; this is not a controlled comparison between versions. The old harness also wrote the 1,000 receipts to stderr in small calls; that cost was removed from the driver and is not presented as a product optimization.

## Desktop and memory bounds

- `desktop-linux-lens-loaded`: sampled peak RSS **134.3 MiB**, CPU **0.000 s** during 10.0 s idle after loading 12,000 messages.
- `desktop-linux-offset-loaded`: sampled peak RSS **358.2 MiB**, CPU **0.040 s** during 10.0 s idle after loading 12,000 messages.

The Linux interface measurement is a snapshot under Xvfb/software OpenGL, with different execution histories; it is not a scrolling or p95/p99 benchmark and does not measure the GPU. External window capture on Windows failed; a later test used the real interfaces, with the user loading messages manually and our UI instrumented. Memory data and the timing comparison's limitations are in [PERFORMANCE-WINDOWS-GUI.md](PERFORMANCE-WINDOWS-GUI.md). The application smoke test runs on both platforms.

Transfers limit the in-flight window to an **estimated 8 MiB or 8,192 messages**, including estimated overhead. A larger message occupies the window on its own. This budget does not limit total RSS: librdkafka, network buffers, decoding, and the GUI allocate additional memory. The configured 16 MiB prefetch limit can also be exceeded by a large message/fetch. Reading/searching retains up to an estimated 128 MiB of records; the table allows up to 20,000 results. Export and replay use streaming.

## Windows stress test with 100,000 messages

Additional fixture (~94 MB of values), one warmup run and three measurements. These tests assess kafunekafka's streaming behavior; Offset Explorer was not compared at this volume.

| Operation | End-to-end time s (min–max) | RSS MiB | Aggregate CPU s |
|---|---:|---:|---:|
| export | 0.999 (0.877–1.076) | 15.4 | 0.656 |
| replay | 6.349 (4.345–7.264) | 39.8 | 6.500 |

Each 100,000-record export was checked against the fixture using per-partition hashes, preserving order, offsets, keys, values, headers, and timestamps. All replays reported 100,000 acknowledgements. The separate integration test also checks destination content and order. Replay time at this volume still varies considerably; this snapshot establishes neither a maximum throughput bound nor an advantage over the competing product at large volumes.

## Changes and validation

- Batch producer with worker callbacks, backpressure, and acknowledgement draining on success, cancellation, and failure. Idempotence and `acks=all` remain enabled.
- Atomic file writing with a 256 KiB buffer; the reader reuses its line buffer and rejects lines larger than 64 MiB.
- Search copies only matching records, preserving the JSON/XML/Avro formatting used during the search.
- The interface avoids full clones per frame and virtualizes the topic list; the consumer uses bounded prefetch with auto-commit disabled.
- Seven unit tests per platform; Kafka integration covering the 12 requirements; an additional test with 2,050 messages of ~4 KiB, three partitions, tombstones, binary/repeated headers, timestamps, order, cancellation, and enqueue failure; clippy with `-D warnings` on both platforms.

During an earlier concurrent build, a Linux fixture exceeded the 10 s metadata deadline; the retry passed. The final version passed the complete test suites on both platforms again. Logs and diagnostic runs remain in `.tools/evidence` and `artifacts/performance/raw`; the tables above use only the named final datasets.

## Reproduction

Install `psutil` in `.tools/perf-python` on Windows (`python -m pip install --target .tools/perf-python psutil`) or in the Linux Python environment. Build with `cargo build --release --locked` and start `docker compose up -d`. Install Offset Explorer separately from the [official website](https://www.kafkatool.com/download.html), respecting its licence; it is not redistributed in the packages.

Create a `kafunekafka-perf` connection in the official CLI and generate the fixture **once, in new topics**. Repeating `fixture` on the same topic appends messages. Adjust the binary paths in the scripts if your installation differs.

```text
commandline -addServerConnection -name kafunekafka-perf -bootstrapServers localhost:19092 -version 4.1 -security plaintext -testConnection true
kafunekafka-bench fixture kafunekafka-perf-balanced-20261014-source 12000 fixture.jsonl kafunekafka-perf-balanced-20261014-destination 3
python scripts/compare_offset_linux.py --trials 5
python scripts/compare_replay.py --trials 5 --offset-project benchmarks/offset-replay.json
python scripts/benchmark.py --binary <kafunekafka-bench> --label <label> --trials 5
python scripts/benchmark_desktop.py --pid <pid> --seconds 10 --label <ui-state>
python scripts/render_performance_report.py --linux-binary <kafunekafka-bench-linux>
```

The general driver uses a 10,000-message fixture and `.tools/perf-input.jsonl` for import: generate that fixture separately, or supply suitable `--topic`, `--destination`, and `--count` values. Summary JSON files and raw output remain in the original measurement environment.

## Measured binary identification

| File | SHA-256 |
|---|---|
| kafunekafka-bench.exe (Windows) | `1a44fa53c817e6c581a5faece38d3a663d8a2a6499726306cf9b8d3fa8d2f0a6` |
| kafunekafka-bench (Linux) | `a599d6b1a3e276235f541bc4582cfa91ea314c49da4d3d9a24fed85d2fb8b4a3` |
