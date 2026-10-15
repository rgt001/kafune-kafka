"""Sample an existing desktop process and children without driving its interface."""
import argparse
import json
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if sys.platform == "win32":
    sys.path.insert(0, str(ROOT / ".tools/perf-python"))
import psutil


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--seconds", type=float, default=10)
    parser.add_argument("--label", required=True)
    parser.add_argument("--uss", action="store_true", help="Also sample exclusive resident memory; slower than RSS")
    args = parser.parse_args()
    parent = psutil.Process(args.pid)
    samples = []
    started = time.perf_counter()
    while time.perf_counter() - started < args.seconds:
        rss = cpu = private = uss = 0
        private_available = False
        for process in [parent] + parent.children(recursive=True):
            try:
                with process.oneshot():
                    memory = process.memory_info()
                    rss += memory.rss
                    if hasattr(memory, "private"):
                        private_available = True
                        private += memory.private
                    if args.uss:
                        uss += process.memory_full_info().uss
                    times = process.cpu_times()
                    cpu += times.user + times.system
            except psutil.Error:
                pass
        samples.append({"seconds": time.perf_counter() - started, "rss_bytes": rss,
                        "private_bytes": private if private_available else None,
                        "uss_bytes": uss if args.uss else None, "cpu_seconds": cpu})
        time.sleep(.1)
    result = {"label": args.label, "pid": args.pid, "executable": parent.exe(),
              "sampling_interval_ms": 100, "samples": samples,
              "peak_rss_bytes_sampled": max(s["rss_bytes"] for s in samples),
              "median_rss_bytes_sampled": statistics.median(s["rss_bytes"] for s in samples),
              "min_rss_bytes_sampled": min(s["rss_bytes"] for s in samples),
              "cpu_seconds_sampled": samples[-1]["cpu_seconds"] - samples[0]["cpu_seconds"],
              "duration_seconds": samples[-1]["seconds"]}
    private_samples = [s["private_bytes"] for s in samples if s["private_bytes"] is not None]
    if private_samples:
        result["median_private_bytes_sampled"] = statistics.median(private_samples)
        result["peak_private_bytes_sampled"] = max(private_samples)
    if args.uss:
        result["median_uss_bytes_sampled"] = statistics.median(s["uss_bytes"] for s in samples)
        result["peak_uss_bytes_sampled"] = max(s["uss_bytes"] for s in samples)
    output = ROOT / "artifacts/performance" / (args.label + ".json")
    output.write_text(json.dumps(result, indent=2))
    print(json.dumps({k: v for k, v in result.items() if k != "samples"}))


if __name__ == "__main__":
    main()
