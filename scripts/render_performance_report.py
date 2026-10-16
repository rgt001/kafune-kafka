"""Render the report from retained, completed benchmark runs."""
import argparse
import hashlib
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "artifacts/performance"


def load(name):
    return json.loads((DATA / (name + ".json")).read_text())


def timing(metric):
    return f'{metric["median"]:.3f} ({metric["min"]:.3f}–{metric["max"]:.3f})'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--linux-binary", type=Path, required=True)
    args = parser.parse_args()
    lines = ["# kafunekafka 0.2.0 — performance measurements\n\nHistorical report from before the public rename to `kafunekafka`. Display names have been updated; results and hashes have been preserved. These figures are not new measurements of version 0.3.0.\n\nThe evidence paths below refer to the original measurement environment. Those local files are not included in this public release.", "",
             "Local measurements from 2026-10-14, compared with Offset Explorer 4.0.5. The optimized version shows a time and memory advantage in the tested CLIs. JVM startup is included in these timings; the advantage cannot be applied directly to an already open interface.", "",
             "## Environment and method", "",
             "AMD Ryzen 7 5700G, 8 cores/16 threads, 32 GB of RAM; Windows 10 Pro 19045 and Ubuntu 22.04 on WSL2. Local Kafka 4.1.0 and Schema Registry 7.8.3. kafunekafka built in `release` mode with thin LTO; Offset Explorer installed from official distributions, with Java 21. Builds and integration tests finished before the final measurements.", "",
             "Each CLI comparison uses one discarded warmup run and five measured runs per product, alternating their order. The table shows the median and minimum–maximum range in seconds. Both programs read the same 12,000 JSON messages (~940 value bytes), with keys, a fixed timestamp, and three partitions of 4,000 messages. The regex search returns the same 120 messages; SHA-256 hashes of normalized records confirm identical content in each read/search pair.", "",
             "`psutil` samples the process and its children every 20 ms. Peak RSS and CPU values are sampling estimates and may miss events between samples; CPU is aggregate thread time, not a percentage. RSS for processes with shared pages does not equal unique physical memory usage. On Linux, stdout and JSONL files are on `/mnt/d` (DrvFS); that filesystem cost is included in total time. End-to-end time includes startup, connection, processing, output, and shutdown.", "",
             "## CLIs — reading and searching", "",
             "| Platform / operation | kafunekafka s (min–max) | Offset Explorer s (min–max) | OE/kafunekafka ratio | kafunekafka / OE RSS MiB | kafunekafka / OE CPU s |",
             "|---|---:|---:|---:|---:|---:|"]
    for platform in ["windows", "linux"]:
        data = load("comparison-" + platform)
        assert data["content_equal"]
        for operation, name in [("view", "read + stdout"), ("search-view", "regex search + stdout")]:
            a, b = (data["summary"][operation][p] for p in ["kafunekafka-0.2.0", "offset-explorer-4.0.5"])
            lines.append(f'| {platform} / {name} | {timing(a["wall_seconds"])} | {timing(b["wall_seconds"])} | {b["wall_seconds"]["median"]/a["wall_seconds"]["median"]:.1f}× | {a["peak_rss_bytes_sampled"]["median"]/2**20:.1f} / {b["peak_rss_bytes_sampled"]["median"]/2**20:.1f} | {a["cpu_seconds_sampled"]["median"]:.3f} / {b["cpu_seconds_sampled"]["median"]:.3f} |')
    lines += ["", "kafunekafka uses the same `Backend` as the desktop app through a development driver. The other side uses the [official Offset Explorer CLI](https://www.kafkatool.com/documentation/commandline.html). This compares two complete CLI executions, including their runtimes. The conditions are local Kafka, plaintext, and JSON; TLS, Avro, WAN latency, and production clusters were not compared for performance.", "",
              "## CLIs — acknowledged replay of 12,000 messages", "",
              "| Platform | kafunekafka end-to-end s (min–max) | Offset Explorer end-to-end s (min–max) | kafunekafka / OE RSS MiB | kafunekafka / OE reported time s |",
              "|---|---:|---:|---:|---:|"]
    for platform in ["windows", "linux"]:
        data = load("replay-comparison-" + platform)
        assert all(r["delivered"] == 12000 for r in data["samples"])
        a, b = (data["summary"][p] for p in ["kafunekafka-0.2.0", "offset-explorer-4.0.5"])
        lines.append(f'| {platform} | {timing(a["wall_seconds"])} | {timing(b["wall_seconds"])} | {a["peak_rss_bytes_sampled"]["median"]/2**20:.1f} / {b["peak_rss_bytes_sampled"]["median"]/2**20:.1f} | {a["operation_seconds_reported"]["median"]:.3f} / {b["operation_seconds_reported"]["median"]:.3f} |')
    lines += ["", "The timers reported by the products have different boundaries: ours includes destination validation and scan setup; Offset Explorer defines its own. These are diagnostic values. In an additional run with Offset Explorer already open on X11, its interface reported **306 ms** for the same 12,000 messages. Our Linux backend took about 295 ms, but these figures do not establish a GUI latency winner. The capture is stored in `.tools/evidence/offset-warm-replay-0.png`.", "",
              "## kafunekafka operations", "",
              "Five runs per operation, with warmup discarded, using 10,000 messages in the original fixture. Export uses JSONL with base64 bytes and `fsync`; import validates the entire file before publishing and waits for acknowledgements. Offset Explorer's format exports individual files and is not equivalent to our JSONL; these formats are not used to claim a direct export/import advantage.", "",
              "| Platform / operation | End-to-end time s (min–max) | RSS MiB |",
              "|---|---:|---:|"]
    for platform in ["windows", "linux"]:
        for operation, metrics in load("release-final-" + platform)["summary"].items():
            lines.append(f'| {platform} / {operation} | {timing(metrics["wall_seconds"])} | {metrics["peak_rss_bytes_sampled"]["median"]/2**20:.1f} |')
    baseline = load("baseline")
    old_import = [s["backend"]["elapsed_seconds"] for s in baseline["samples"] if s["operation"] == "import"]
    current = load("release-final-windows")
    new_import = [s["backend"]["elapsed_seconds"] for s in current["samples"] if s["operation"] == "import" and not s["warmup"]]
    lines += ["", f'The original import acknowledged one message at a time: the two 10,000-message runs (warmup and first measurement) have a median internal time of **{statistics.median(old_import):.2f} s**. The final version has a median internal time of **{statistics.median(new_import):.3f} s**, across five measurements. The number of repetitions differs; this is not a controlled comparison between versions. The old harness also wrote the 1,000 receipts to stderr in small calls; that cost was removed from the driver and is not presented as a product optimization.', "",
              "## Desktop and memory bounds", ""]
    for name in ["desktop-linux-lens-loaded", "desktop-linux-offset-loaded"]:
        path = DATA / (name + ".json")
        if path.exists():
            row = load(name)
            lines.append(f'- `{name}`: sampled peak RSS **{row["peak_rss_bytes_sampled"]/2**20:.1f} MiB**, CPU **{row["cpu_seconds_sampled"]:.3f} s** during {row["duration_seconds"]:.1f} s idle after loading 12,000 messages.')
    windows_note = "External window capture on Windows failed; a later test used the real interfaces, with the user loading messages manually and our UI instrumented. Memory data and the timing comparison's limitations are in [PERFORMANCE-WINDOWS-GUI.md](PERFORMANCE-WINDOWS-GUI.md)." if (DATA / "gui-windows-comparison.json").exists() else "Native Windows capture failed during the automation session; the quantitative GUI comparison is limited to Linux."
    lines += ["", "The Linux interface measurement is a snapshot under Xvfb/software OpenGL, with different execution histories; it is not a scrolling or p95/p99 benchmark and does not measure the GPU. " + windows_note + " The application smoke test runs on both platforms.", "",
              "Transfers limit the in-flight window to an **estimated 8 MiB or 8,192 messages**, including estimated overhead. A larger message occupies the window on its own. This budget does not limit total RSS: librdkafka, network buffers, decoding, and the GUI allocate additional memory. The configured 16 MiB prefetch limit can also be exceeded by a large message/fetch. Reading/searching retains up to an estimated 128 MiB of records; the table allows up to 20,000 results. Export and replay use streaming.", "",
              "## Windows stress test with 100,000 messages", "",
              "Additional fixture (~94 MB of values), one warmup run and three measurements. These tests assess kafunekafka's streaming behavior; Offset Explorer was not compared at this volume.", "",
              "| Operation | End-to-end time s (min–max) | RSS MiB | Aggregate CPU s |",
              "|---|---:|---:|---:|"]
    for operation, metrics in load("stress-100k-windows")["summary"].items():
        lines.append(f'| {operation} | {timing(metrics["wall_seconds"])} | {metrics["peak_rss_bytes_sampled"]["median"]/2**20:.1f} | {metrics["cpu_seconds_sampled"]["median"]:.3f} |')
    verification = load("stress-100k-archives")
    assert verification["status"] == "passed"
    lines += ["", "Each 100,000-record export was checked against the fixture using per-partition hashes, preserving order, offsets, keys, values, headers, and timestamps. All replays reported 100,000 acknowledgements. The separate integration test also checks destination content and order. Replay time at this volume still varies considerably; this snapshot establishes neither a maximum throughput bound nor an advantage over the competing product at large volumes.", "",
              "## Changes and validation", "",
              "- Batch producer with worker callbacks, backpressure, and acknowledgement draining on success, cancellation, and failure. Idempotence and `acks=all` remain enabled.",
              "- Atomic file writing with a 256 KiB buffer; the reader reuses its line buffer and rejects lines larger than 64 MiB.",
              "- Search copies only matching records, preserving the JSON/XML/Avro formatting used during the search.",
              "- The interface avoids full clones per frame and virtualizes the topic list; the consumer uses bounded prefetch with auto-commit disabled.",
              "- Seven unit tests per platform; Kafka integration covering the 12 requirements; an additional test with 2,050 messages of ~4 KiB, three partitions, tombstones, binary/repeated headers, timestamps, order, cancellation, and enqueue failure; clippy with `-D warnings` on both platforms.", "",
              "During an earlier concurrent build, a Linux fixture exceeded the 10 s metadata deadline; the retry passed. The final version passed the complete test suites on both platforms again. Logs and diagnostic runs remain in `.tools/evidence` and `artifacts/performance/raw`; the tables above use only the named final datasets.", "",
              "## Reproduction", "",
              "Install `psutil` in `.tools/perf-python` on Windows (`python -m pip install --target .tools/perf-python psutil`) or in the Linux Python environment. Build with `cargo build --release --locked` and start `docker compose up -d`. Install Offset Explorer separately from the [official website](https://www.kafkatool.com/download.html), respecting its licence; it is not redistributed in the packages.", "",
              "Create a `kafunekafka-perf` connection in the official CLI and generate the fixture **once, in new topics**. Repeating `fixture` on the same topic appends messages. Adjust the binary paths in the scripts if your installation differs.", "",
              "```text",
              "commandline -addServerConnection -name kafunekafka-perf -bootstrapServers localhost:19092 -version 4.1 -security plaintext -testConnection true",
              "kafunekafka-bench fixture kafunekafka-perf-balanced-20261014-source 12000 fixture.jsonl kafunekafka-perf-balanced-20261014-destination 3",
              "python scripts/compare_offset_linux.py --trials 5",
              "python scripts/compare_replay.py --trials 5 --offset-project benchmarks/offset-replay.json",
              "python scripts/benchmark.py --binary <kafunekafka-bench> --label <label> --trials 5",
              "python scripts/benchmark_desktop.py --pid <pid> --seconds 10 --label <ui-state>",
              "python scripts/render_performance_report.py --linux-binary <kafunekafka-bench-linux>",
              "```", "",
              "The general driver uses a 10,000-message fixture and `.tools/perf-input.jsonl` for import: generate that fixture separately, or supply suitable `--topic`, `--destination`, and `--count` values. Summary JSON files and raw output remain in the original measurement environment.", "",
              "## Measured binary identification", "",
              "| File | SHA-256 |", "|---|---|"]
    paths = [ROOT / "target/release/kafunekafka-bench.exe", args.linux_binary]
    hashes = []
    for path in paths:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        lines.append(f'| {path.name} ({"Windows" if path.suffix == ".exe" else "Linux"}) | `{digest}` |')
        hashes.append({"binary": str(path), "sha256": digest})
    (DATA / "measured-binaries.json").write_text(json.dumps(hashes, indent=2))
    (ROOT / "PERFORMANCE.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
