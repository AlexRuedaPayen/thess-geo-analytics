from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests

from thess_geo_analytics.core.constants import CDSE_HTTP_TIMEOUT, HTTP_RETRIES, HTTP_BACKOFF
from thess_geo_analytics.core.settings import VERBOSE


@dataclass(frozen=True)
class HttpConfig:
    timeout: int = CDSE_HTTP_TIMEOUT
    retries: int = HTTP_RETRIES
    backoff_seconds: float = HTTP_BACKOFF


class HttpClient:
    """
    Very small helper: consistent retries/backoff for GET/POST.
    Keeps services clean; not a heavy abstraction.
    """

    def __init__(self, cfg: HttpConfig | None = None) -> None:
        self.cfg = cfg or HttpConfig()

    def get(self, url: str, *, headers: Optional[Dict[str, str]] = None, stream: bool = False) -> requests.Response:
        return self._request("GET", url, headers=headers, stream=stream)

    def post(self, url: str, *, headers: Optional[Dict[str, str]] = None, json: Any = None, data: Any = None) -> requests.Response:
        return self._request("POST", url, headers=headers, json=json, data=data)

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        last_exc: Exception | None = None

        for attempt in range(self.cfg.retries + 1):
            try:
                if VERBOSE:
                    print(f"[HTTP] {method} {url} (attempt {attempt+1}/{self.cfg.retries+1})")

                r = requests.request(method, url, timeout=self.cfg.timeout, **kwargs)

                # Retry only on transient 5xx
                if 500 <= r.status_code <= 599:
                    time.sleep(self.cfg.backoff_seconds * (attempt + 1))
                    continue

                r.raise_for_status()
                return r

            except Exception as e:
                last_exc = e
                time.sleep(self.cfg.backoff_seconds * (attempt + 1))

        raise RuntimeError(f"HTTP {method} failed after retries: {url}. Last error: {last_exc}")
