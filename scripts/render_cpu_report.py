import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Render the Linux CPU experiment report")
    parser.add_argument("experiment", type=Path, nargs="?", default=Path("reports/p8-cpu-experiment.json"))
    parser.add_argument("--output", type=Path, default=Path("reports/p8-cpu-experiment.md"))
    args = parser.parse_args()
    data = json.loads(args.experiment.read_text(encoding="utf-8"))
    fault_cpu = data["fault_cpu_rate"][0]["value"][1]
    recovery_cpu = data["recovery_cpu_rate"][0]["value"][1]
    lines = [
        "# P8 Linux CPU 故障实验",
        "",
        "## 场景与对象",
        "",
        f"- 时间：{data['started_at']} 至 {data['finished_at']}（UTC）",
        f"- 目标：`{data['container']}` 内 uvicorn PID 1，实例 `inventory-linux-1`",
        f"- 限额：{data['cpu_limit']} 核；真实链路为 order → inventory",
        "- 故障：后台忙循环和请求内短时 CPU 计算，通过 `/fault` 开关启停",
        "",
        "## 现象",
        "",
        "| 阶段 | 五次端到端耗时（秒） | 进程 CPU |",
        "|---|---|---|",
        f"| 基线 | {', '.join(f'{value:.4f}' for value in data['baseline_seconds'])} | 约 0.004 核 |",
        f"| 故障 | {', '.join(f'{value:.4f}' for value in data['fault_seconds'])} | Prometheus {float(fault_cpu):.4f} 核 |",
        f"| 恢复 | {', '.join(f'{value:.4f}' for value in data['recovery_seconds'])} | Prometheus {float(recovery_cpu):.4f} 核 |",
        "",
        "## 推断与证据边界",
        "",
        "- inventory 自身 CPU 使用与慢请求同步升高，Trace 显示 inventory server Span 约 0.50–0.58 秒；故障解除后请求和进程 CPU 均恢复。",
        "- 证据属于目标容器进程，不使用 macOS 开发机或 APM 平台负载作为根因。",
        "- RSS 稳定只说明没有观察到持续内存增长，不能严格排除所有内存问题。",
        "- `top/vmstat` 展示 Linux VM 总体状态，仅作背景；根因判断使用进程指标、Trace 和故障日志交叉验证。",
        "- Prometheus 故障后恢复采样等待了 25 秒，避免 20 秒 rate 窗口继续包含故障期。",
        "",
        "## 网络、连接与 I/O 边界",
        "",
        "- 真实实验中 RabbitMQ 空闲 channel 关闭曾使日志发布触发 HTTP 500；发布器已加锁检测并重连，说明业务请求与遥测依赖会相互影响。",
        "- HTTPX 异步请求不会手动实现 epoll；Python 事件循环由底层 I/O 多路复用支撑，Pika 同步调用则位于独立进程或有界执行路径。",
        "- RabbitMQ/ClickHouse 的写成功与 ack 语义沿用 P5 实测，不将函数返回成功夸大为跨系统恰好一次或掉电持久性证明。",
        "- 未额外注入网络、内存或磁盘故障；这些主题仅结合现有链路和输出讲解。",
        "",
        "## 原始命令输出",
        "",
    ]
    for name, output in data["commands"].items():
        lines.extend([f"### {name}", "", "```text", output.strip(), "```", ""])
    lines.extend([
        "## 复现",
        "",
        "```bash",
        "docker compose up -d --build instrumented-order instrumented-inventory",
        "uv run python scripts/run_cpu_fault_experiment.py",
        "uv run --env-file .env python scripts/consume_telemetry.py --max-messages 10000 --metrics-port 0",
        "uv run python scripts/render_cpu_report.py",
        "```",
        "",
        "Agent 报告：`reports/investigation-1b24a6a7-3309-4048-8498-54c54d1b75e7.md`。",
    ])
    args.output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(args.output)


if __name__ == "__main__":
    main()
