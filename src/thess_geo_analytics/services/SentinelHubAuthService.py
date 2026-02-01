from __future__ import annotations
import os
from sentinelhub import SHConfig

class SentinelHubAuthService:
    def __init__(self)->None:
        pass

    def build_config(self) -> SHConfig:
        client_id = os.getenv("SH_CLIENT_ID")
        client_secret = os.getenv("SH_CLIENT_SECRET")
        if not client_id or not client_secret:
            raise EnvironmentError("Missing SH_CLIENT_ID / SH_CLIENT_SECRET in environment (.env).")

        config = SHConfig()
        config.sh_client_id = client_id
        config.sh_client_secret = client_secret
        return config
