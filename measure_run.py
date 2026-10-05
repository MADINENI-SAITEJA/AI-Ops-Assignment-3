"""Ubuntu HOST tool: real cgroup-v2 resource sampling plus raw top logs.

No Python packages are needed on the host. It does not create/configure clusters.
"""
import argparse
import csv
import json
import subprocess
import sys
import threading
import time
from pathlib import Path


def inspect_container(name):
    result = subprocess.run(["docker", "inspect", name], check=True,
                            capture_output=True, text=True)
    state = json.loads(result.stdout)[0]
    pid = state["State"]["Pid"]
    if not state["State"]["Running"] or pid <= 0:
        raise RuntimeError(f"Container must already be running: {name}")
    entries = Path(f"/proc/{pid}/cgroup").read_text().splitlines()
    rel = next((entry.split("::", 1)[1] for entry in entries if entry.startswith("0::")), None)
    if rel is None:
        raise RuntimeError("This monitor requires cgroup v2 (default on Ubuntu 22.04/24.04).")
    folder = Path("/sys/fs/cgroup") / rel.lstrip("/")
    if not (folder / "cpu.stat").is_file():
        raise RuntimeError(f"Cannot read cgroup for {name}: {folder}")
    return {"name": name, "folder": folder, "image": state["Image"],
            "cpu_quota_cores": state["HostConfig"]["NanoCpus"] / 1_000_000_000,
            "memory_limit_bytes": state["HostConfig"]["Memory"]}


def stat_map(path):
    return {parts[0]: int(parts[1]) for parts in
            (line.split() for line in path.read_text().splitlines()) if len(parts) == 2}


def snapshot(container):
    folder = container["folder"]
    cpu = stat_map(folder / "cpu.stat")["usage_usec"]
    total = int((folder / "memory.current").read_text())
    inactive = stat_map(folder / "memory.stat").get("inactive_file", 0)
    return cpu, total, max(0, total - inactive)


def dump_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--metrics", required=True, help="Host path of pipeline's result JSON")
    p.add_argument("--containers", required=True, nargs="+")
    p.add_argument("--workers", required=True, nargs="+")
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("command", nargs=argparse.REMAINDER)
    args = p.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command or not set(args.workers).issubset(args.containers):
        p.error("Provide a command after --, and include workers in --containers")
    if args.interval <= 0:
        p.error("--interval must be positive")
    path = Path(args.metrics).resolve()
    if path.exists():
        raise FileExistsError(f"Metrics already exist; use a new run name: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    containers = [inspect_container(name) for name in args.containers]
    samples, errors, stop = [], [], threading.Event()

    def sampler():
        previous = {c["name"]: (time.monotonic(), snapshot(c)[0]) for c in containers}
        while not stop.wait(args.interval):
            epoch = time.time()
            sample = {"epoch": epoch, "containers": {}}
            try:
                for c in containers:
                    current = time.monotonic()
                    usage, total, working = snapshot(c)
                    before, old_usage = previous[c["name"]]
                    # 100% = one fully used CPU core, as with top/docker stats.
                    cpu_percent = 100 * (usage - old_usage) / 1_000_000 / (current - before)
                    previous[c["name"]] = current, usage
                    sample["containers"][c["name"]] = {
                        "cpu_percent": max(0.0, cpu_percent), "memory_current_bytes": total,
                        "memory_working_set_bytes": working}
                samples.append(sample)
            except (OSError, KeyError) as exc:
                errors.append(str(exc))

    command_log = path.with_name(path.stem + "_command.log")
    top_log = path.with_name(path.stem + "_top.log")
    thread = threading.Thread(target=sampler, daemon=True)
    wrapper_start = time.perf_counter()
    with top_log.open("w") as top_stream, command_log.open("w") as run_stream:
        top = subprocess.Popen(["top", "-b", "-d", "1", "-w", "256"],
                               stdout=top_stream, stderr=subprocess.STDOUT)
        child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                 text=True, bufsize=1, errors="replace")
        thread.start()
        try:
            for line in child.stdout:
                print(line, end="", flush=True)
                run_stream.write(line)
            code = child.wait()
        except KeyboardInterrupt:
            child.terminate()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
            code = 130
        finally:
            stop.set()
            thread.join(timeout=5)
            top.terminate()
            try:
                top.wait(timeout=5)
            except subprocess.TimeoutExpired:
                top.kill()
    wrapper_seconds = time.perf_counter() - wrapper_start
    dump_json(path.with_name(path.stem + "_resource_samples.json"),
              {"samples": samples, "errors": errors, "command": command,
               "command_exit_code": code, "wrapper_seconds": wrapper_seconds})
    if code != 0:
        print(f"Command failed with exit code {code}. See {command_log}", file=sys.stderr)
        raise SystemExit(code)
    if not path.is_file():
        raise RuntimeError("Pipeline did not produce metrics; no performance result is claimed")
    result = json.loads(path.read_text())
    first, last = result["pipeline_started_epoch"], result["pipeline_finished_epoch"]
    window = [s for s in samples if first <= s["epoch"] <= last]
    if not window or errors:
        raise RuntimeError("Resource sampling incomplete. Keep logs and rerun before reporting peaks.")

    def peaks(names):
        return {
            "peak_cpu_percent": max(sum(s["containers"][n]["cpu_percent"] for n in names) for s in window),
            "peak_memory_working_set_bytes": max(sum(s["containers"][n]["memory_working_set_bytes"] for n in names) for s in window),
            "peak_memory_charged_bytes": max(sum(s["containers"][n]["memory_current_bytes"] for n in names) for s in window)}
    result["resources"] = {"cluster": peaks(args.containers), "workers": peaks(args.workers),
                            "sample_interval_seconds": args.interval, "sample_count": len(window),
                            "definition": "Observed aggregate container peaks DURING ingestion-to-export. "
                              "CPU 100%=one core; memory working set=current-inactive_file. "
                              "Whole-cluster includes coordinator and driver; workers is the two data nodes only. "
                              "Sampling may miss sub-second spikes; top raw logs retained.",
                            "container_budgets": [{k: v for k, v in c.items() if k != "folder"}
                                                  for c in containers]}
    dump_json(path, result)
    fields = ["epoch", "cluster_cpu_percent", "cluster_memory_working_set_bytes",
              "workers_cpu_percent", "workers_memory_working_set_bytes"]
    with path.with_name(path.stem + "_resources.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for sample in window:
            writer.writerow({"epoch": sample["epoch"], **{
                f"{scope}_{metric}": sum(sample["containers"][n][metric] for n in names)
                for scope, names in (("cluster", args.containers), ("workers", args.workers))
                for metric in ("cpu_percent", "memory_working_set_bytes")}})
    print(f"Real runtime and resource measurements saved: {path}")


if __name__ == "__main__":
    main()
