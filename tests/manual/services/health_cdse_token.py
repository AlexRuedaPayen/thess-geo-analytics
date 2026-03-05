from __future__ import annotations

from thess_geo_analytics.services.CdseTokenService import CdseTokenService
from _common import print_header, print_kv, run_step, write_report, print_summary


def main() -> int:
    print_header("CDSE HEALTH — TOKEN")

    results = []

    svc, r = run_step("Create CdseTokenService", lambda: CdseTokenService())
    results.append(r)
    if svc is None:
        write_report("health_cdse_token", results)
        return 1

    t1, r = run_step("get_token() first", lambda: svc.get_token())
    results.append(r)

    def _reuse():
        return svc.get_token()
    t2, r = run_step("get_token() reuse", _reuse)
    results.append(r)

    def _force_refresh():
        return svc.get_token(force_refresh=True)
    t3, r = run_step("get_token(force_refresh=True)", _force_refresh)
    results.append(r)

    if t1:
        print_kv(token1_prefix=t1[:20] + "...")
    if t2:
        print_kv(token2_prefix=t2[:20] + "...")
    if t3:
        print_kv(token3_prefix=t3[:20] + "...")

    write_report("health_cdse_token", results)
    print_summary(results)
    return 0 if all(r.ok for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())