"""Measure native Windows process resources, retaining every trial and output.

Install psutil in .tools/perf-python; no changes to the global Python environment.
Run only when builds have finished. No broker/topic is deleted by this driver.
"""
import argparse
import hashlib
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if sys.platform == "win32":
    sys.path.insert(0, str(ROOT / ".tools/perf-python"))
import psutil


def measure(argv, tag, timeout=300):
    folder = ROOT / "artifacts/performance/raw"
    folder.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    with (folder / f"{tag}.stdout").open("wb") as out, (folder / f"{tag}.stderr").open("wb") as err:
        child = subprocess.Popen([str(a) for a in argv], stdout=out, stderr=err, cwd=ROOT)
        process = psutil.Process(child.pid)
        peak_rss = peak_private = 0
        private_available = False
        cpu_by_pid = {}
        processes = [process]
        last_children = 0
        while child.poll() is None:
            try:
                if time.perf_counter() - last_children > .5:
                    processes = [process] + process.children(recursive=True)
                    last_children = time.perf_counter()
                rss = private = 0
                for item in processes:
                    try:
                        with item.oneshot():
                            mem = item.memory_info()
                            rss += mem.rss
                            if hasattr(mem, "private"):
                                private_available = True
                                private += mem.private
                            cpu = item.cpu_times()
                            cpu_by_pid[item.pid] = cpu.user + cpu.system
                    except psutil.Error:
                        pass
                peak_rss = max(peak_rss, rss)
                peak_private = max(peak_private, private)
            except psutil.Error:
                pass
            if time.perf_counter() - started > timeout:
                child.kill()
                child.wait()
                raise RuntimeError(f"{tag} exceeded {timeout}s; see retained logs")
            time.sleep(.02)
    seconds = time.perf_counter() - started
    stdout = folder / f"{tag}.stdout"
    stderr = (folder / f"{tag}.stderr").read_text(errors="replace")
    if child.returncode:
        raise RuntimeError(f"{tag} exit {child.returncode}: {stderr[-1500:]}")
    result = {"trial": tag, "argv": [str(a) for a in argv], "wall_seconds": seconds,
              "cpu_seconds_sampled": sum(cpu_by_pid.values()), "peak_rss_bytes_sampled": peak_rss,
              "peak_private_bytes_sampled": peak_private if private_available else None, "stdout_bytes": stdout.stat().st_size,
              "stdout_lines": sum(1 for _ in stdout.open("rb")), "stdout_sha256": hashlib.sha256(stdout.read_bytes()).hexdigest()}
    for line in reversed(stderr.splitlines()):
        try:
            result["backend"] = json.loads(line)
            break
        except ValueError:
            pass
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--operations", default="snapshot,read,search,export,import,replay,archive")
    parser.add_argument("--count", type=int, default=10000)
    parser.add_argument("--topic", default="kafunekafka-perf-20261014-source")
    parser.add_argument("--destination", default="kafunekafka-perf-20261014-destination")
    parser.add_argument("--trials", type=int, default=5)
    args = parser.parse_args()
    results = []
    for operation in args.operations.split(","):
        for index in range(-1, args.trials):
            tag = f"{args.label}-{operation}-{index}"
            path = ROOT / ".tools" / (f"perf-input-{args.count}.jsonl" if operation == "import" and args.count != 10000 else "perf-input.jsonl" if operation == "import" else f"{tag}.jsonl")
            result = measure([args.binary, operation, args.topic, args.count, path, args.destination], tag)
            result.update(operation=operation, label=args.label, warmup=index < 0)
            results.append(result)
            (ROOT / f"artifacts/performance/{args.label}.json").write_text(json.dumps({"samples": results}, indent=2))
            print(json.dumps({k: result[k] for k in ("trial", "wall_seconds", "peak_private_bytes_sampled")}), flush=True)
    summary = {}
    for operation in args.operations.split(","):
        rows = [r for r in results if r["operation"] == operation and not r["warmup"]]
        summary[operation] = {k: {"median": statistics.median(r[k] for r in rows), "min": min(r[k] for r in rows), "max": max(r[k] for r in rows)} for k in ("wall_seconds", "cpu_seconds_sampled", "peak_rss_bytes_sampled", "peak_private_bytes_sampled") if all(r[k] is not None for r in rows)}
    (ROOT / f"artifacts/performance/{args.label}.json").write_text(json.dumps({"samples": results, "summary": summary}, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
