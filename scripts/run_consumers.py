import argparse
import signal
import subprocess
import sys
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Run independent RabbitMQ consumer processes")
    parser.add_argument("--workers", type=int, choices=(1, 2, 4), default=1)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--prefetch", type=int, default=20)
    parser.add_argument("--flush-interval", type=float, default=1)
    parser.add_argument("--max-pending", type=int, default=2000)
    parser.add_argument("--stop-flush-timeout", type=float, default=5)
    parser.add_argument("--shutdown-timeout", type=float, default=10)
    parser.add_argument("--run-id", default="local")
    args = parser.parse_args()

    script = Path(__file__).with_name("consume_telemetry.py")
    processes = []
    for index in range(args.workers):
        command = [
            sys.executable,
            str(script),
            "--batch-size", str(args.batch_size),
            "--prefetch", str(args.prefetch),
            "--flush-interval", str(args.flush_interval),
            "--max-pending", str(args.max_pending),
            "--stop-flush-timeout", str(args.stop_flush_timeout),
            "--metrics-port", str(8767 + index),
            "--worker-id", f"{args.run_id}-worker-{index + 1}",
        ]
        processes.append(subprocess.Popen(command))
    stopping = False

    def stop_children(signum, frame) -> None:
        nonlocal stopping
        if stopping:
            return
        stopping = True
        for process in processes:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)

    for signal_name in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signal_name, stop_children)
    deadline = time.monotonic() + args.shutdown_timeout
    while any(process.poll() is None for process in processes):
        if not stopping:
            time.sleep(0.2)
            continue
        if time.monotonic() >= deadline:
            for process in processes:
                if process.poll() is None:
                    process.kill()
            break
        time.sleep(0.05)
    exit_codes = [process.wait() for process in processes]
    if any(code != 0 for code in exit_codes):
        raise SystemExit(f"consumer exit codes: {exit_codes}")


if __name__ == "__main__":
    main()
