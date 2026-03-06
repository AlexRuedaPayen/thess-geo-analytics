from __future__ import annotations

import json
import os
import shutil
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple


REPORTS_ROOT = Path("tests/manual/services/_reports")
REPORTS_SESSION_ENV = "THESS_MANUAL_REPORTS_SESSION_DIR"


@dataclass
class StepResult:
    name: str
    ok: bool
    started_at: float
    ended_at: float
    seconds: float
    message: str = ""
    error_type: str = ""
    error: str = ""
    traceback: str = ""
    extra: Dict[str, Any] | None = None


def now() -> float:
    return time.time()


def fmt_seconds(s: float) -> str:
    return f"{s:.3f}s"


def print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def print_kv(**kwargs: Any) -> None:
    for k, v in kwargs.items():
        print(f"{k:>22}: {v}")


def get_session_reports_dir() -> Path:
    raw = os.environ.get(REPORTS_SESSION_ENV)
    if raw:
        return Path(raw)
    # fallback if a single health file is run directly
    return REPORTS_ROOT / "session_standalone"


def init_session_reports_dir(session_name: str | None = None, *, clean: bool = True) -> Path:
    if session_name is None:
        session_name = f"session_{time.strftime('%Y%m%d_%H%M%S')}"

    session_dir = REPORTS_ROOT / session_name

    if clean and session_dir.exists():
        shutil.rmtree(session_dir)

    session_dir.mkdir(parents=True, exist_ok=True)
    os.environ[REPORTS_SESSION_ENV] = str(session_dir)

    return session_dir


def run_step(name: str, fn: Callable[[], Any]) -> Tuple[Optional[Any], StepResult]:
    started = now()
    try:
        out = fn()
        ended = now()
        res = StepResult(
            name=name,
            ok=True,
            started_at=started,
            ended_at=ended,
            seconds=ended - started,
            message="ok",
            extra={},
        )
        print(f"[OK] {name} ({fmt_seconds(res.seconds)})")
        return out, res
    except Exception as e:
        ended = now()
        tb = traceback.format_exc()
        res = StepResult(
            name=name,
            ok=False,
            started_at=started,
            ended_at=ended,
            seconds=ended - started,
            message="failed",
            error_type=type(e).__name__,
            error=str(e),
            traceback=tb,
            extra={},
        )
        print(f"[ERR] {name} ({fmt_seconds(res.seconds)}) → {type(e).__name__}: {e}")
        print(tb)
        return None, res


def write_report(name: str, results: list[StepResult], extra: Dict[str, Any] | None = None) -> Path:
    reports_dir = get_session_reports_dir()
    reports_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "name": name,
        "when_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "python": sys.version,
        "cwd": str(Path.cwd()),
        "env": {
            "THESS_RUN_ROOT": os.environ.get("THESS_RUN_ROOT"),
            REPORTS_SESSION_ENV: os.environ.get(REPORTS_SESSION_ENV),
        },
        "extra": extra or {},
        "results": [r.__dict__ for r in results],
        "summary": {
            "ok": sum(1 for r in results if r.ok),
            "failed": sum(1 for r in results if not r.ok),
            "total": len(results),
        },
    }

    out = reports_dir / f"{name}.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"[REPORT] wrote {out}")
    return out


def print_summary(results: list[StepResult]) -> None:
    ok = [r for r in results if r.ok]
    bad = [r for r in results if not r.ok]
    print("\n--- SUMMARY ---")
    print(f"OK: {len(ok)} | FAILED: {len(bad)} | TOTAL: {len(results)}")
    if bad:
        print("\nFailed steps:")
        for r in bad:
            print(f" - {r.name}: {r.error_type}: {r.error}")