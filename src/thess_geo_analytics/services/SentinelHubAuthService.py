from __future__ import annotations
import os
from sentinelhub import SHConfig

class SentinelHubAuthService:
    def build_config(self) -> SHConfig:
        client_id = os.getenv("SH_CLIENT_ID")
        client_secret = os.getenv("SH_CLIENT_SECRET")
        if not client_id or not client_secret:
            raise EnvironmentError("Missing SH_CLIENT_ID / SH_CLIENT_SECRET in environment (.env).")

        config = SHConfig()
        config.sh_client_id = client_id
        config.sh_client_secret = client_secret

        # Force CDSE (even if env vars are missing)
        config.sh_base_url = os.getenv("SH_BASE_URL", "https://sh.dataspace.copernicus.eu")
        config.sh_token_url = os.getenv(
            "SH_TOKEN_URL",
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
        )
        return config
