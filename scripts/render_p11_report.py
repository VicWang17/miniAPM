import json
from pathlib import Path


def main():
    tests = json.loads(Path("reports/p11-rule-tests.json").read_text())
    live = json.loads(Path("reports/p11-live.json").read_text())
    assert tests["command_returncode"] == 0 and live["loaded_rules"] == 10
    rows = [
        ("正常", "无告警", "无告警", "0 错误，正常耗时"),
        ("持续错误", "快烧与慢烧触发", "1h2m 快烧；6h2m 慢烧", "20/120=16.67%，BR=16.67"),
        ("轻微毛刺", "不触发", "6h2m 无告警", "短窗口很快恢复"),
        ("短暂严重故障", "允许不触发", "快烧/慢烧均未触发", "多窗口可能漏掉短故障"),
        ("低流量", "守卫抑制", "6h2m 无告警", "QPS=0.05，不解释为正常"),
        ("稳定 Gauge 从 2 到 4", "3σ 触发，固定阈值不触发", "2h2m 符合预期", "均值=2、σ=0、阈值=2；固定阈值=5"),
        ("Gauge 自然抖动", "不触发", "2h2m 无告警", "2/3 交替，3σ 上限高于当前值"),
        ("5% 慢请求", "延迟 SLO 触发，P95 对照不触发", "10m 符合预期", "慢请求 5%>1%；P95 与 99% 目标不同"),
        ("持续错误后恢复", "慢烧恢复", "6h2m 触发，6h40m 无告警", "Counter 停止增长，30m 短窗口恢复"),
    ]
    table = "\n".join("| " + " | ".join(row) + " |" for row in rows)
    content = f"""# P11 异常发现对照实验

对象固定为 dataset `018f2f70-0000-7000-8000-000000000606`、local、order、`POST /orders`、simulated。可用性目标 99%，允许失败 1%，教学预算周期 720h；快烧阈值 14.4，慢烧阈值 6，短窗 QPS>0.1。Prometheus 保留期仍为 7 天，不计算真实 30 天余额。

## 确定性规则结果

| 场景 | 预期 | 实际 | 口径/限制 |
|---|---|---|---|
{table}

这些输入是 promtool 构造序列，不是真实业务观测。1h/6h 逻辑由完整序列验证，没有真实等待这些时长。

## 真实接线

- Prometheus 实际加载 {live['loaded_rules']} 条规则。
- Alertmanager 测试通知进入 `{live['receiver']}`。
- 调查数从 {live['investigations_before']} 到 {live['investigations_after']}，实验通知未触发 Agent。
- 该通知是短时接线探针，不是真实长窗口 SLO 触发。

## 限制

σ=0 时任何大于均值的值都可能触发；异常进入参考窗口会抬升未来阈值。统计偏离不证明用户受影响，3σ 不是故障概率。低流量守卫可能漏报，缺数据不等于正常。Hive/ClickHouse 未参与触发，所有策略各有目标，不以告警数量判优。
"""
    Path("reports/p11-comparison.md").write_text(content)
    print("reports/p11-comparison.md")


if __name__ == "__main__":
    main()
