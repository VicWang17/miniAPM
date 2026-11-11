import json
import subprocess
from pathlib import Path

def main():
    result = subprocess.run(["docker", "compose", "exec", "-T", "prometheus", "promtool", "test", "rules",
        "/etc/prometheus/miniapm-slo-rules-test.yml"], text=True, capture_output=True)
    output = {"command_returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr,
              "limitations": ["promtool series are deterministic inputs, not real business observations",
                              "formal 6h and 30d behavior is not waited out in real time"]}
    Path("reports/p11-rule-tests.json").write_text(json.dumps(output, indent=2))
    print(result.stdout or result.stderr)
    if result.returncode:
        raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
