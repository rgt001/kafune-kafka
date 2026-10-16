"""Compare supported Offset Explorer CLI with the desktop Backend on Linux.
No producer changes, deletions, bytecode inspection or proprietary code reuse.
"""
import hashlib
import json
import re
import statistics
import time
import sys
import argparse
from pathlib import Path
from benchmark import ROOT, measure

PLATFORM = "windows" if sys.platform == "win32" else "linux"
OFFSET = ROOT/".tools/offset-explorer/app/commandline.exe" if PLATFORM == "windows" else Path.home()/".local/opt/offset-explorer-bench/commandline"
LENS = ROOT/"target/release/kafunekafka-bench.exe" if PLATFORM == "windows" else Path.home()/".cache/kafunekafka-target/release/kafunekafka-bench"
TOPIC = "kafunekafka-perf-balanced-20261014-source"
COUNT = 12000


def normalized_output(trial):
    lines=(ROOT/f"artifacts/performance/raw/{trial}.stdout").read_text().splitlines()
    records=[line for line in lines if re.match(r"^[0-2]:\d+ key-\d+ \{",line)]
    return records, hashlib.sha256("\n".join(sorted(records)).encode()).hexdigest()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--trials",type=int,default=5)
    args=parser.parse_args()
    rows=[]
    for operation in ["view","search-view"]:
        offset=[OFFSET,"-viewMessages","-serverConnection","kafunekafka-perf","-topic",TOPIC,"-first",str(COUNT//3),"-keyFormat","text","-valueFormat","text","-format","#partition#:#offset# #key# #value#"]
        if operation == "search-view": offset += ["-valueFilterRegex",".*target.*"]
        commands={"offset-explorer-4.0.5":offset,"kafunekafka-0.2.0":[LENS,operation,TOPIC,str(COUNT)]}
        for index in range(-1,args.trials):
            checks={}
            for product in (list(commands) if index%2==0 else list(reversed(commands))):
                tag=f"balanced-{PLATFORM}-{product}-{operation}-{index}"
                result=measure(commands[product],tag)
                records,digest=normalized_output(tag)
                expected=COUNT if operation=="view" else COUNT//100
                if len(records)!=expected: raise RuntimeError(f"{tag}: {len(records)} != {expected}; retained stdout")
                result.update(product=product,operation=operation,warmup=index<0,records=len(records),normalized_sha256=digest)
                checks[product]=digest
                rows.append(result)
                (ROOT/f"artifacts/performance/comparison-{PLATFORM}.json").write_text(json.dumps({"samples":rows},indent=2))
                print(json.dumps({"trial":tag,"wall_seconds":result["wall_seconds"],"rss_mib":result["peak_rss_bytes_sampled"]/1024**2}),flush=True)
                time.sleep(.2)
            if len(set(checks.values()))!=1: raise RuntimeError(f"Different message content: {checks}")
    summary={}
    for operation in ["view","search-view"]:
        summary[operation]={}
        for product in commands:
            values=[r for r in rows if r["product"]==product and r["operation"]==operation and not r["warmup"]]
            summary[operation][product]={k:{"median":statistics.median(r[k] for r in values),"min":min(r[k] for r in values),"max":max(r[k] for r in values)} for k in ["wall_seconds","cpu_seconds_sampled","peak_rss_bytes_sampled"]}
    (ROOT/f"artifacts/performance/comparison-{PLATFORM}.json").write_text(json.dumps({"samples":rows,"summary":summary,"platform":PLATFORM,"count":COUNT,"partitions":3,"content_equal":True},indent=2))
    print(json.dumps(summary,indent=2))


if __name__=="__main__": main()
