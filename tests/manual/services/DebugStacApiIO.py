from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

from pystac_client.stac_api_io import StacApiIO


class DebugStacApiIO(StacApiIO):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._req_n = 0

    def request(
        self,
        href: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> str:
        self._req_n += 1
        n = self._req_n

        t0 = time.time()
        print("\n" + "=" * 90)
        print(f"[STAC-HTTP #{n}] {method} {href}")
        if parameters:
            # pystac_client often puts query params here for GET
            print("[params]", json.dumps(parameters, indent=2, default=str)[:2000])
        if headers:
            # don’t dump auth tokens if any ever appear
            safe_headers = {k: ("<redacted>" if "auth" in k.lower() else v) for k, v in headers.items()}
            print("[headers]", json.dumps(safe_headers, indent=2)[:2000])

        try:
            txt = super().request(href, method=method, headers=headers, parameters=parameters)
            dt = time.time() - t0
            print(f"[STAC-HTTP #{n}] OK in {dt:.2f}s | bytes={len(txt)}")
            return txt
        except Exception as e:
            dt = time.time() - t0
            print(f"[STAC-HTTP #{n}] FAIL in {dt:.2f}s | {type(e).__name__}: {e}")
            raise