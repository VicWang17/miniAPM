import argparse
import json
import os
import platform
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx
from prometheus_client.parser import text_string_to_metric_families

from miniapm.rabbitmq_publisher import RabbitMQPublisher

QUEUES = ("miniapm.logs", "miniapm.spans")


def queue_depth(publisher: RabbitMQPublisher) -> int:
    return sum(
        publisher.channel.queue_declare(queue=queue, passive=True).method.message_count
        for queue in QUEUES
    )


def publish_input(publisher: RabbitMQPublisher, observations: Path) -> dict:
    counts = {}
    started = time.monotonic()
    for routing_key in ("logs", "spans"):
        with (observations / f"{routing_key}.jsonl").open(encoding="utf-8") as source:
            events = [json.loads(line) for line in source]
        for event in events:
            publisher.publish(routing_key, event)
        counts[routing_key] = len(events)
    duration = time.monotonic() - started
    return {
        "events": sum(counts.values()),
        "by_type": counts,
        "duration_seconds": duration,
        "events_per_second": sum(counts.values()) / duration,
    }


def process_resources(parent_pid: int) -> dict[int, dict[str, float]]:
    output = subprocess.check_output(
        ["ps", "-ax", "-o", "pid=,ppid=,%cpu=,rss="], text=True
    )
    resources = {}
    for line in output.splitlines():
        pid, ppid, cpu, rss = line.split()
        if int(ppid) == parent_pid:
            resources[int(pid)] = {"cpu_percent": float(cpu), "rss_bytes": int(rss) * 1024}
    return resources


def worker_metrics(workers: int) -> tuple[dict[str, int], list[dict]]:
    acknowledged = {}
    buckets = []
    with httpx.Client(trust_env=False, timeout=2) as client:
        for index in range(workers):
            response = client.get(f"http://127.0.0.1:{8767 + index}/metrics")
            response.raise_for_status()
            for family in text_string_to_metric_families(response.text):
                for sample in family.samples:
                    if sample.name == "miniapm_intake_acknowledged_total":
                        worker_id = sample.labels["worker_id"]
                        acknowledged[worker_id] = acknowledged.get(worker_id, 0) + int(sample.value)
                    elif sample.name == "miniapm_ingestion_delay_seconds_bucket":
                        buckets.append({"le": sample.labels["le"], "count": sample.value})
    return acknowledged, buckets


def histogram_p95(buckets: list[dict]) -> float:
    totals = {}
    for bucket in buckets:
        le = float(bucket["le"])
        totals[le] = totals.get(le, 0) + bucket["count"]
    total = totals.get(float("inf"), 0)
    if not total:
        raise AssertionError("ingestion delay histogram has no samples")
    target = total * 0.95
    cumulative = 0
    for le in sorted(totals):
        cumulative += totals[le]
        if cumulative >= target:
            return le
    raise AssertionError("unreachable histogram quantile")


def run_benchmark(args, workers: int, observations: Path) -> dict:
    publisher = RabbitMQPublisher()
    assert queue_depth(publisher) == 0, "telemetry queues must be empty before benchmark"
    publication = publish_input(publisher, observations)
    assert queue_depth(publisher) == publication["events"]
    command = [
        sys.executable,
        str(Path(__file__).with_name("run_consumers.py")),
        "--workers", str(workers),
        "--batch-size", str(args.batch_size),
        "--prefetch", str(args.prefetch),
        "--flush-interval", str(args.flush_interval),
        "--max-pending", str(args.max_pending),
        "--run-id", f"benchmark-{workers}",
    ]
    started = time.monotonic()
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    max_backlog = publication["events"]
    resources = {}
    try:
        while process.poll() is None:
            backlog = queue_depth(publisher)
            max_backlog = max(max_backlog, backlog)
            for pid, values in process_resources(process.pid).items():
                previous = resources.setdefault(pid, {"cpu_percent_max": 0, "rss_bytes_max": 0})
                previous["cpu_percent_max"] = max(previous["cpu_percent_max"], values["cpu_percent"])
                previous["rss_bytes_max"] = max(previous["rss_bytes_max"], values["rss_bytes"])
                previous["cpu_percent_last"] = values["cpu_percent"]
                previous["rss_bytes_last"] = values["rss_bytes"]
            if backlog == 0:
                break
            if time.monotonic() - started > args.timeout:
                raise TimeoutError(f"{workers} consumers did not drain input")
            time.sleep(0.1)
        time.sleep(0.3)
        acknowledged, buckets = worker_metrics(workers)
        assert sum(acknowledged.values()) == publication["events"], acknowledged
        delay_p95 = histogram_p95(buckets)
        process.send_signal(signal.SIGTERM)
        output, _ = process.communicate(timeout=20)
        if process.returncode != 0:
            raise RuntimeError(output)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
    duration = time.monotonic() - started
    assert queue_depth(publisher) == 0
    publisher.close()
    return {
        "workers": workers,
        "duration_seconds": duration,
        "logical_events_written": sum(acknowledged.values()),
        "logical_write_events_per_second": sum(acknowledged.values()) / duration,
        "ingestion_delay_p95_seconds_upper_bound": delay_p95,
        "max_backlog_events": max_backlog,
        "final_backlog_events": 0,
        "per_worker_acknowledged": acknowledged,
        "per_process_resources": resources,
        "total_prefetch": workers * args.prefetch,
        "total_max_pending_events": workers * args.max_pending,
        "publication": publication,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare 1/2/4 consumer processes")
    parser.add_argument("dataset_directory", type=Path)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--prefetch", type=int, default=100)
    parser.add_argument("--flush-interval", type=float, default=1)
    parser.add_argument("--max-pending", type=int, default=2000)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--output", type=Path, default=Path("reports/p5-consumer-benchmark.json"))
    args = parser.parse_args()
    observations = args.dataset_directory / "observations"
    results = [run_benchmark(args, workers, observations) for workers in (1, 2, 4)]
    report = {
        "machine": {
            "platform": platform.platform(),
            "logical_cpu_count": os.cpu_count(),
            "memory_bytes": int(subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True)),
        },
        "input_dataset": str(args.dataset_directory),
        "method_notes": [
            "Input is fully published before consumer timing starts; publication throughput is recorded separately.",
            "Ingestion delay is consumer receipt minus event_time and therefore includes historical event age.",
            "P95 is reported as the first matching cumulative histogram upper bound, not an exact latency.",
            "CPU percentage is the ps lifetime average sampled during the run; RSS is sampled resident memory."
        ],
        "per_process": {
            "batch_size": args.batch_size,
            "prefetch": args.prefetch,
            "max_pending_events": args.max_pending,
            "flush_interval_seconds": args.flush_interval,
        },
        "results": results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
