from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

SCRIPTS = [
    ROOT / "health_cdse_http.py",
    ROOT / "health_cdse_token.py",
    ROOT / "health_cdse_scene_catalog.py",
    ROOT / "health_cdse_stac_item.py",
]

def main() -> int:
    exe = sys.executable
    print("\n=== RUN ALL MANUAL HEALTH CHECKS ===")

    worst_rc = 0
    for script in SCRIPTS:
        print("\n" + "-" * 80)
        print(f"Running: {script.as_posix()}")
        print("-" * 80)

        if not script.exists():
            print(f"[SKIP] Missing file: {script}")
            worst_rc = max(worst_rc, 1)
            continue

        p = subprocess.run([exe, str(script)])
        worst_rc = max(worst_rc, p.returncode)

    return worst_rc

if __name__ == "__main__":
    raise SystemExit(main())