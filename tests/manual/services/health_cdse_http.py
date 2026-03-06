from __future__ import annotations

from thess_geo_analytics.core.constants import CDSE_TOKEN_URL, CDSE_CATALOG_STAC_URL
from thess_geo_analytics.core.HttpClient import HttpClient, HttpConfig
from _common import print_header, print_kv, run_step, write_report, print_summary


def main() -> int:
    print_header("CDSE HEALTH — HTTP BASELINE")

    results = []
    http = HttpClient(HttpConfig())

    print_kv(token_url=CDSE_TOKEN_URL, catalogue_url=CDSE_CATALOG_STAC_URL, timeout=http.cfg.timeout, retries=http.cfg.retries)

    # A cheap GET: landing page / root (no auth)
    def _get_catalogue_root():
        return http.get(CDSE_CATALOG_STAC_URL)

    resp, r = run_step("GET catalogue root", _get_catalogue_root)
    results.append(r)

    if resp is not None:
        print_kv(status=resp.status_code, content_type=resp.headers.get("content-type"))

    write_report("health_cdse_http", results)
    print_summary(results)
    return 0 if all(r.ok for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())