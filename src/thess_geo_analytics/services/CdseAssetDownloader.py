from __future__ import annotations

import os
from pathlib import Path

import requests

from thess_geo_analytics.core.constants import CDSE_DOWNLOAD_TIMEOUT
from thess_geo_analytics.core.settings import VERBOSE
from thess_geo_analytics.services.CdseTokenService import CdseTokenService


class CdseAssetDownloader:
    def __init__(self, token_service: CdseTokenService) -> None:
        self.token_service = token_service

    def download(self, href: str, out_path: Path) -> Path:
        out_path.parent.mkdir(parents=True, exist_ok=True)

        token = self.token_service.get_token()
        headers = {"Authorization": f"Bearer {token}"}

        if VERBOSE:
            print(f"[DL] {href} -> {out_path}")

        with requests.get(href, headers=headers, stream=True, timeout=CDSE_DOWNLOAD_TIMEOUT) as r:
            r.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        return out_path

    @staticmethod
    def smoke_test() -> None:
        print("=== CdseAssetDownloader Smoke Test ===")
        print("This smoke test is SAFE by default (no download).")

        if os.environ.get("SMOKE_DOWNLOAD", "0") not in {"1", "true", "yes"}:
            print("[SKIP] Set SMOKE_DOWNLOAD=1 to test downloading.")
            print("✓ Smoke test OK (skipped download)")
            return

        href = os.environ.get("SMOKE_HREF")
        if not href:
            raise SystemExit("Set SMOKE_HREF env var to a valid asset href to download.")

        out_path = Path("outputs/tmp/smoke_download.bin")

        downloader = CdseAssetDownloader(CdseTokenService())
        p = downloader.download(href, out_path)

        if not p.exists() or p.stat().st_size == 0:
            raise RuntimeError("Downloaded file missing or empty")

        print("[OK] Downloaded:", p, "bytes:", p.stat().st_size)
        print("✓ Smoke test OK")


if __name__ == "__main__":
    CdseAssetDownloader.smoke_test()
