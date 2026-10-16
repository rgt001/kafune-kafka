# Windows desktop interfaces — kafunekafka and Offset Explorer

Historical report from before the public rename to `kafunekafka`. Display names have been updated; results and hashes have been preserved. These figures are not new measurements of version 0.3.0.

The evidence paths below refer to the original measurement environment. Those local files are not included in this public release.

Test conducted on 2026-10-14 with 12,000 messages from the same local topic and three partitions.

## Memory with messages loaded

| Metric | kafunekafka 0.2.0 | Offset Explorer 4.0.5 |
|---|---:|---:|
| Unique resident memory (USS) | 46.9 MiB | 416.6 MiB |
| Total resident memory (RSS) | 291.6 MiB | 694.1 MiB |
| Private committed memory | 115.9 MiB | 628.9 MiB |

Values are medians of samples taken during an approximately 15-second idle interval per process. Observed CPU time during that interval was 0.000 s for both, within the counters' resolution. A zero reading does not prove a complete absence of activity.

USS measures unique resident memory; RSS also includes shared pages and mapped modules. Private committed memory includes pages that may not be resident in RAM. These metrics have different meanings, as described in the [psutil documentation](https://psutil.io/api/#psutil.Process.memory_full_info). Measurements used psutil 7.2.2 and real Windows processes. VRAM was not measured.

After rereading, kafunekafka was below Offset Explorer on all three metrics. This is a snapshot of the observed state, with different usage and garbage-collection histories; it is neither a guaranteed upper bound nor a comparison of freshly launched processes. At an earlier stage, kafunekafka's total resident memory was higher than Offset Explorer's. kafunekafka's memory map showed approximately 141 MiB in two NVIDIA modules (`nvgpucomp64.dll` and `nvoglv64.dll`), in addition to other allocations. This cost is also part of the desktop interface.

## Data loading time

- **kafunekafka, five reads in the same window:** median **201.8 ms**, mean **340.8 ms**, range **185.7–906.6 ms**. One read took approximately 907 ms; it was retained in the calculation.
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

## Version 0.2.1 optimization — comparison between versions

Five sessions of the previous version were alternated with five of the new version, each with one discarded warmup read and nine measured reads. This gives **45 reads of 12,000 records per version**, in the same native interface, including formatted JSON and the table. No slow values were removed; detailed phase tracing was disabled during the comparison.

| Time until data and layout were ready | 0.2.0 | 0.2.1 |
|---|---:|---:|
| Median | 185.7 ms | 62.8 ms |
| Mean | 237.1 ms | 62.8 ms |
| Minimum | 171.1 ms | 57.2 ms |
| Maximum | 1650.4 ms | 71.5 ms |

The median decreased by 66.2% (2.96×). Including framebuffer capture, the new median was 70.5 ms, with a range of 63.9–145.4 ms. This capture includes its own overhead and does not measure the physical instant when the monitor displayed the frame.

Tracing before the fix found 100–115 ms spent destroying the consumer after collecting records. An intermediate session recorded 760 ms between task dispatch and the new thread starting. This locates the delay but does not identify which Windows component caused the wait. Version 0.2.1 keeps a consumer and a worker, removes the assignment on completion/cancellation, refreshes metadata and watermarks, reduces `fetch.wait.max.ms` from 100 to 10 ms, and formats the first message in the read worker itself. JSON/XML and local Avro do not require HTTP/TLS initialization; the registry client is created only when used.

**The new 62.8 ms mean was below the 180 ms reported for Offset Explorer in the earlier manual test.** The reduction between our versions was measured with the same timer; the Offset Explorer comparison still uses different timers and sessions. Offset Explorer's idle RAM measurement was not repeated, and the memory table above belongs to version 0.2.0. Resource peaks for the ten new processes are recorded in the JSON, along with individual samples.

Nine unit tests and clippy passed on Windows and Linux. Real integration validation covers the 12 requirements, bulk import/replay, and reused-connection regressions: refreshed bounds after new publishes, cancellation followed by another range, different partitions/offsets, error recovery, and cluster switching. Local Kafka had topic-creation timeouts during initial validation; logs were preserved, both containers were restarted without recreation, and tests were repeated.

Evidence: `artifacts/performance/gui-windows-optimization.json`, `gui-ab-*-same-window.json`, `raw/gui-*.stdout` and `raw/gui-*.stderr`. Reproduction:

```powershell
python scripts/compare_native_ui.py --baseline <runner-0.2.0> --binary target/release/kafunekafka-ui-bench.exe
```
