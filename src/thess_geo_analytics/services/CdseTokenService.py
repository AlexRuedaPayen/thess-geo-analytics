from __future__ import annotations

import time
from typing import Optional

from thess_geo_analytics.core.constants import CDSE_TOKEN_URL
from thess_geo_analytics.core.HttpClient import HttpClient, HttpConfig
from thess_geo_analytics.core.settings import CDSE_USERNAME, CDSE_PASSWORD, CDSE_TOTP


class CdseTokenService:
    def __init__(self, http: HttpClient | None = None) -> None:
        # Current access token (if any)
        self._token: Optional[str] = None
        # Unix timestamp when token expires (server-side)
        self._expires_at: float = 0.0
        # Refresh N seconds before expiry
        self._safety_margin: int = 60
        # How many times we've actually fetched a token from CDSE
        self._fetch_count: int = 0

        self.http = http or HttpClient(HttpConfig())

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _need_refresh(self) -> bool:
        """
        Decide whether we need to fetch a new token.

        Returns True if:
          - we have no token yet, or
          - current time is beyond (expires_at - safety_margin).
        """
        if not self._token:
            return True

        now = time.time()
        return now >= (self._expires_at - self._safety_margin)

    def _fetch_token(self) -> str:
        """
        Actually call the CDSE token endpoint and update internal state.
        """
        if not CDSE_USERNAME or not CDSE_PASSWORD:
            raise EnvironmentError("Missing CDSE_USERNAME / CDSE_PASSWORD in environment (.env).")

        data = {
            "client_id": "cdse-public",
            "grant_type": "password",
            "username": CDSE_USERNAME,
            "password": CDSE_PASSWORD,
        }
        if CDSE_TOTP:
            data["totp"] = CDSE_TOTP

        r = self.http.post(CDSE_TOKEN_URL, data=data)
        payload = r.json()

        token = payload.get("access_token")
        if not token:
            raise RuntimeError(f"Token response missing access_token. Payload: {payload}")

        expires_in = float(payload.get("expires_in", 3600))

        self._fetch_count += 1
        self._token = token
        self._expires_at = time.time() + expires_in

        print(f"[DEBUG] _fetch_token called #{self._fetch_count}, expires_in={int(expires_in)}")

        return self._token

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def get_token(self, *, force_refresh: bool = False) -> str:
        """
        Get a valid access token.

        - If force_refresh=True, always fetch a new one.
        - Otherwise, reuse existing until near expiry, then refresh.
        """
        if force_refresh or self._need_refresh():
            return self._fetch_token()
        # mypy/pyright: we know _token is not None if _need_refresh() is False
        return self._token  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Smoke test with refresh behaviour
    # ------------------------------------------------------------------
    @staticmethod
    def smoke_test() -> None:
        import time as _time

        print("=== CdseTokenService Smoke Test (with refresh) ===")
        svc = CdseTokenService()

        # STEP 1: First token fetch
        print("\n[STEP 1] First get_token() – should fetch once")
        t1 = svc.get_token()
        print("Token 1 prefix:", t1[:20], "...")
        print("Fetch count:", svc._fetch_count)

        # STEP 2: Second call should reuse token (no new fetch)
        print("\n[STEP 2] Second get_token() – should reuse token (no new fetch)")
        t2 = svc.get_token()
        print("Token 2 prefix:", t2[:20], "...")
        print("Fetch count:", svc._fetch_count, "(expected still 1)")

        # STEP 3: Simulate expiry and ensure we fetch again
        print("\n[STEP 3] Simulate expiry and call get_token() – should trigger a refresh")
        svc._expires_at = _time.time() - 10  # force expiry
        t3 = svc.get_token()
        print("Token 3 prefix:", t3[:20], "...")
        print("Fetch count:", svc._fetch_count, "(expected 2)")

        print("\n[RESULT]")
        if svc._fetch_count >= 2:
            print("✓ Refresh logic is working: fetched, reused, then re-fetched after simulated expiry.")
        else:
            print("✗ Refresh logic did NOT re-fetch after simulated expiry – check _need_refresh().")


if __name__ == "__main__":
    CdseTokenService.smoke_test()