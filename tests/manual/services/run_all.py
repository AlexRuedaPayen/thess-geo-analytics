from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

from _common import init_session_reports_dir


def main() -> None:
    session_name = f"session_{time.strftime('%Y%m%d_%H%M%S')}"
    session_dir = init_session_reports_dir(session_name, clean=True)

    print("\n=== RUN ALL MANUAL HEALTH CHECKS ===")
    print(f"[REPORT SESSION] {session_dir}")

    files = [
        Path("tests/manual/services/health_cdse_http.py"),
        Path("tests/manual/services/health_cdse_token.py"),
        Path("tests/manual/services/health_cdse_scene_catalog_deep.py"),
        Path("tests/manual/services/health_cdse_stac_item.py"),
    ]

    for file in files:
        print("\n" + "-" * 80)
        print(f"Running: {file.resolve()}")
        print("-" * 80)
        subprocess.run([sys.executable, str(file)], check=False)


if __name__ == "__main__":
    main()