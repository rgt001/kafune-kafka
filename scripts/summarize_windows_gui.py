"""Record the Windows GUI measurements and the human-reported OE timing."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "artifacts/performance"


def main():
    def load(name):
        return json.loads((DATA / (name + ".json")).read_text())
    timing = load("native-ui-windows-lens-same-window")
    previous = load("gui-windows-comparison") if (DATA / "gui-windows-comparison.json").exists() else {}
    memory = {"kafunekafka": load("desktop-windows-lens-final-0"),
              "offset_explorer": load("desktop-windows-offset-final-1")}
    result = {"platform": "windows", "date": "2026-10-14", "count": 12000,
              "topic": "kafunekafka-perf-balanced-20261014-source",
              "kafunekafka_timing": timing["summary"],
              "offset_explorer_timing": {"mean_seconds_reported_by_user": .180,
                                         "requested_repetitions": 5, "individual_samples": None,
                                         "source": "User read the GUI status bar and reported a mean of 180ms after the five-read request."},
              "memory": {product: {k: row[k] for k in ["pid", "executable", "duration_seconds", "median_rss_bytes_sampled", "median_uss_bytes_sampled", "median_private_bytes_sampled", "cpu_seconds_sampled"]} for product, row in memory.items()},
              "method": "Two real Windows desktop processes, same topic and 12000 retained records, user-operated loading verified by supplied screenshots. Memory sampled at 100ms in an idle 15s interval per process after reloads. Native kafunekafka test runner performs five reads in one window after discarded warmup using the production UI/worker.",
              "limits": "OE timer boundaries are defined by that application; human reported a mean without individual samples. kafunekafka timer includes dispatch, read, selected JSON formatting and table layout. No matched click-to-present latency benchmark. Process histories, foreground/background state and viewport sizes differ. USS/RSS depend on OS paging; Java garbage collection affects retained memory. No GPU memory measurement.",
              "measured_version": "0.2.0",
              "production_windows_binary_sha256": previous.get("production_windows_binary_sha256") or hashlib.sha256((ROOT / "target/release/kafunekafka.exe").read_bytes()).hexdigest(),
              "test_runner_sha256": timing["binary_sha256"]}
    (DATA / "gui-windows-comparison.json").write_text(json.dumps(result, indent=2))
    rows = []
    for label, field in [("Unique resident memory (USS)", "median_uss_bytes_sampled"),
                         ("Total resident memory (RSS)", "median_rss_bytes_sampled"),
                         ("Private committed memory", "median_private_bytes_sampled")]:
        values = [result["memory"][p][field] / 2**20 for p in ["kafunekafka", "offset_explorer"]]
        rows.append(f"| {label} | {values[0]:.1f} MiB | {values[1]:.1f} MiB |")
    data_ready = timing["summary"]["data_ready"]
    report = f'''# Windows desktop interfaces — kafunekafka and Offset Explorer

Historical report from before the public rename to `kafunekafka`. Display names have been updated; results and hashes have been preserved. These figures are not new measurements of version 0.3.0.

The evidence paths below refer to the original measurement environment. Those local files are not included in this public release.

Test conducted on 2026-10-14 with 12,000 messages from the same local topic and three partitions.

## Memory with messages loaded

| Metric | kafunekafka 0.2.0 | Offset Explorer 4.0.5 |
|---|---:|---:|
{chr(10).join(rows)}

Values are medians of samples taken during an approximately 15-second idle interval per process. Observed CPU time during that interval was 0.000 s for both, within the counters' resolution. A zero reading does not prove a complete absence of activity.

USS measures unique resident memory; RSS also includes shared pages and mapped modules. Private committed memory includes pages that may not be resident in RAM. These metrics have different meanings, as described in the [psutil documentation](https://psutil.io/api/#psutil.Process.memory_full_info). Measurements used psutil 7.2.2 and real Windows processes. VRAM was not measured.

After rereading, kafunekafka was below Offset Explorer on all three metrics. This is a snapshot of the observed state, with different usage and garbage-collection histories; it is neither a guaranteed upper bound nor a comparison of freshly launched processes. At an earlier stage, kafunekafka's total resident memory was higher than Offset Explorer's. kafunekafka's memory map showed approximately 141 MiB in two NVIDIA modules (`nvgpucomp64.dll` and `nvoglv64.dll`), in addition to other allocations. This cost is also part of the desktop interface.

## Data loading time

- **kafunekafka, five reads in the same window:** median **{data_ready['median']*1000:.1f} ms**, mean **{data_ready['mean']*1000:.1f} ms**, range **{data_ready['min']*1000:.1f}–{data_ready['max']*1000:.1f} ms**. One read took approximately 907 ms; it was retained in the calculation.
- **Offset Explorer:** a **180 ms mean reported by the user**, read from the interface's timing indicator after five rereads were requested. The five individual values were not recorded.

Our timer runs from read dispatch through receipt of the records, JSON formatting of the selected message, and table layout. It excludes the initial connection and process startup. The second timer captures the rendered framebuffer and includes screenshot readback overhead. Offset Explorer defines its own timing boundaries. These figures therefore do not constitute an identical click-to-present measurement.

**This test does not establish lower Windows GUI latency for kafunekafka.** The observations favor kafunekafka's memory usage in the tested state. The 14–17× gains measured in the CLIs do not apply to an already open interface.

## Evidence and reproduction

The two screenshots supplied by the user confirm 12,000 messages in the same topic. The initial Offset Explorer screenshot shows hexadecimal bytes and 310 ms; the user was asked to change Key/Value to String and repeat before subsequently reporting the 180 ms mean. The images are stored in `artifacts/performance/ui-windows-*-12000-user.png`.

`gui-windows-comparison.json` retains the comparison; `desktop-windows-*-final-*.json` retains each memory sample; `native-ui-windows-lens-same-window.json` retains the reads and timer boundaries. Earlier samples were preserved and were not used in the table above.

```powershell
cargo build --release --locked --bin kafunekafka-ui-bench
python scripts/benchmark_native_ui.py --binary target/release/kafunekafka-ui-bench.exe --same-window
python scripts/benchmark_desktop.py --pid <PID> --seconds 15 --uss --label <state>
python scripts/summarize_windows_gui.py
```

The runner uses the real UI and worker, with instrumentation restricted to smoke mode. The normal interface was unchanged. Seven unit tests and clippy with `-D warnings` passed after instrumentation. External window-capture comparison failed (`FrameArrived timed out` and `window capture timed out`); the user loaded Offset Explorer manually. Automatic UI control was interrupted with Escape, and no further UI actions were performed afterward.
'''
    if (DATA / "gui-windows-optimization.json").exists():
        optimization = load("gui-windows-optimization")
        result["optimization_0_2_1"] = optimization
        (DATA / "gui-windows-comparison.json").write_text(json.dumps(result, indent=2))
        old = optimization["summary"]["baseline"]["data_ready"]
        new = optimization["summary"]["optimized"]["data_ready"]
        frame = optimization["summary"]["optimized"]["frame_captured"]
        report += f'''
## Version 0.2.1 optimization — comparison between versions

Five sessions of the previous version were alternated with five of the new version, each with one discarded warmup read and nine measured reads. This gives **{new['count']} reads of 12,000 records per version**, in the same native interface, including formatted JSON and the table. No slow values were removed; detailed phase tracing was disabled during the comparison.

| Time until data and layout were ready | 0.2.0 | 0.2.1 |
|---|---:|---:|
| Median | {old['median']*1000:.1f} ms | {new['median']*1000:.1f} ms |
| Mean | {old['mean']*1000:.1f} ms | {new['mean']*1000:.1f} ms |
| Minimum | {old['min']*1000:.1f} ms | {new['min']*1000:.1f} ms |
| Maximum | {old['max']*1000:.1f} ms | {new['max']*1000:.1f} ms |

The median decreased by {100*(1-new['median']/old['median']):.1f}% ({old['median']/new['median']:.2f}×). Including framebuffer capture, the new median was {frame['median']*1000:.1f} ms, with a range of {frame['min']*1000:.1f}–{frame['max']*1000:.1f} ms. This capture includes its own overhead and does not measure the physical instant when the monitor displayed the frame.

Tracing before the fix found 100–115 ms spent destroying the consumer after collecting records. An intermediate session recorded 760 ms between task dispatch and the new thread starting. This locates the delay but does not identify which Windows component caused the wait. Version 0.2.1 keeps a consumer and a worker, removes the assignment on completion/cancellation, refreshes metadata and watermarks, reduces `fetch.wait.max.ms` from 100 to 10 ms, and formats the first message in the read worker itself. JSON/XML and local Avro do not require HTTP/TLS initialization; the registry client is created only when used.

**The new {new['mean']*1000:.1f} ms mean was below the 180 ms reported for Offset Explorer in the earlier manual test.** The reduction between our versions was measured with the same timer; the Offset Explorer comparison still uses different timers and sessions. Offset Explorer's idle RAM measurement was not repeated, and the memory table above belongs to version 0.2.0. Resource peaks for the ten new processes are recorded in the JSON, along with individual samples.

Nine unit tests and clippy passed on Windows and Linux. Real integration validation covers the 12 requirements, bulk import/replay, and reused-connection regressions: refreshed bounds after new publishes, cancellation followed by another range, different partitions/offsets, error recovery, and cluster switching. Local Kafka had topic-creation timeouts during initial validation; logs were preserved, both containers were restarted without recreation, and tests were repeated.

Evidence: `artifacts/performance/gui-windows-optimization.json`, `gui-ab-*-same-window.json`, `raw/gui-*.stdout` and `raw/gui-*.stderr`. Reproduction:

```powershell
python scripts/compare_native_ui.py --baseline <runner-0.2.0> --binary target/release/kafunekafka-ui-bench.exe
```
'''
    (ROOT / "PERFORMANCE-WINDOWS-GUI.md").write_text(report, encoding="utf-8")
    print(json.dumps({"memory": result["memory"], "latency": data_ready}, indent=2))


if __name__ == "__main__":
    main()
