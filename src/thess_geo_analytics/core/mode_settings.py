from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Mapping, Optional
import os


_ALLOWED_MODES = {"dev", "deep"}
_DEFAULT_MODE = "deep"


def _normalize_mode(raw_cfg: Mapping[str, Any]) -> str:
    """
    Read and validate top-level `mode` field.
    Missing => 'deep'. Invalid => ValueError.
    """
    mode = raw_cfg.get("mode", _DEFAULT_MODE)
    if mode not in _ALLOWED_MODES:
        raise ValueError(
            f"Invalid mode={mode!r}. Allowed values: {sorted(_ALLOWED_MODES)}"
        )
    return mode


@dataclass(frozen=True)
class ModeSettings:
    """
    Encapsulates dev vs deep behavior.

    - Given raw YAML dicts (scene_catalog / assets_manifest / ndvi),
      returns *effective* dicts for the current mode.
    - Also resolves band_resolution and parallelism knobs.
    """

    mode: str

    # ------------- constructors / helpers -------------

    @classmethod
    def from_raw_config(cls, raw_cfg: Mapping[str, Any]) -> "ModeSettings":
        return cls(mode=_normalize_mode(raw_cfg))

    @property
    def is_dev(self) -> bool:
        return self.mode == "dev"

    @property
    def is_deep(self) -> bool:
        return self.mode == "deep"

    # ------------- Scene catalog -------------

    def effective_scene_catalog(
        self, raw: Mapping[str, Any]
    ) -> Dict[str, Any]:
        """
        Deep mode: pass-through (no clamping).
        Dev mode: clamp to your “demo” values, without duplicating YAML.
        """
        sc = deepcopy(dict(raw))

        if self.is_deep:
            return sc

        # Dev targets
        MAX_DAYS_DEV = 1469          # ~4 years
        MAX_ITEMS_DEV = 400
        MAX_CLOUD_DEV = 20.0         # you want 20% in dev
        N_ANCHORS_DEV = 24
        WINDOW_DAYS_DEV = 21

        # Only clamp where the key exists and is numeric.
        if isinstance(sc.get("days"), int):
            sc["days"] = min(sc["days"], MAX_DAYS_DEV)

        if isinstance(sc.get("max_items"), int):
            sc["max_items"] = min(sc["max_items"], MAX_ITEMS_DEV)

        # cloud_cover_max: be at least as strict in dev
        cc = sc.get("cloud_cover_max")
        if isinstance(cc, (int, float)):
            sc["cloud_cover_max"] = min(float(cc), MAX_CLOUD_DEV)
        else:
            sc["cloud_cover_max"] = MAX_CLOUD_DEV

        if isinstance(sc.get("n_anchors"), int):
            sc["n_anchors"] = min(sc["n_anchors"], N_ANCHORS_DEV)

        if isinstance(sc.get("window_days"), int):
            sc["window_days"] = min(sc["window_days"], WINDOW_DAYS_DEV)

        return sc

    # ------------- Assets manifest -------------

    def effective_assets_manifest(
        self, raw: Mapping[str, Any]
    ) -> Dict[str, Any]:
        """
        Deep mode: pass-through.
        Dev mode:
          - keep around ~4 years by clamping the date range if needed
          - keep logical max_scenes (null) if small, otherwise clamp
          - cap download_n to 10
          - disable uploads/deletes by default
        """
        am = deepcopy(dict(raw))

        if self.is_deep:
            return am

        # ---- time coverage (~4 years) ----
        # We respect YAML if already ~4 years; otherwise clamp start date.
        date_fmt = "%Y-%m-%d"
        date_start_str = am.get("date_start")
        date_end_str = am.get("date_end")

        try:
            if date_start_str and date_end_str:
                ds = datetime.strptime(date_start_str, date_fmt)
                de = datetime.strptime(date_end_str, date_fmt)
                max_span = timedelta(days=1469)  # ~4 years

                if de - ds > max_span:
                    new_ds = de - max_span
                    # Don’t go earlier than original start
                    if new_ds < ds:
                        new_ds = ds
                    am["date_start"] = new_ds.strftime(date_fmt)
        except Exception:
            # If parsing fails, silently keep YAML dates
            pass

        # ---- max_scenes ----
        MAX_SCENES_DEV = 400  # safety cap
        max_scenes = am.get("max_scenes")
        if isinstance(max_scenes, int):
            am["max_scenes"] = min(max_scenes, MAX_SCENES_DEV)
        # if None, leave as None – “keep all logically” as you requested

        # ---- download_n ----
        DOWNLOAD_N_DEV = 10
        dn = am.get("download_n")
        if isinstance(dn, int):
            am["download_n"] = min(dn, DOWNLOAD_N_DEV)
        else:
            am["download_n"] = DOWNLOAD_N_DEV

        # ---- uploads / deletion ----
        am["upload_to_gcs"] = False
        am["delete_local_after_upload"] = False

        return am

    # ------------- NDVI composites (optional) -------------

    def effective_ndvi_composites(
        self, raw: Mapping[str, Any]
    ) -> Dict[str, Any]:
        """
        Deep mode: pass-through.
        Dev mode: keep “a handful” of COGs & stats.
        """
        ndvi = deepcopy(dict(raw))

        if self.is_deep:
            return ndvi

        MAX_SCENES_PER_PERIOD_DEV = 20
        MIN_SCENES_PER_MONTH_DEV = 2

        msp = ndvi.get("max_scenes_per_period")
        if isinstance(msp, int):
            ndvi["max_scenes_per_period"] = min(msp, MAX_SCENES_PER_PERIOD_DEV)
        elif msp is None:
            ndvi["max_scenes_per_period"] = MAX_SCENES_PER_PERIOD_DEV

        mmin = ndvi.get("min_scenes_per_month")
        if isinstance(mmin, int):
            ndvi["min_scenes_per_month"] = min(mmin, MIN_SCENES_PER_MONTH_DEV)
        elif mmin is None:
            ndvi["min_scenes_per_month"] = MIN_SCENES_PER_MONTH_DEV

        ndvi["download_missing"] = False
        ndvi["upload_composites_to_gcs"] = False

        return ndvi

    # ------------- Band resolution -------------

    def effective_band_resolution(
        self, raw_assets_cfg: Mapping[str, Any]
    ) -> int:
        """
        Base YAML: assets_manifest.band_resolution = 10 (deep mode).
        Dev mode: force 20 m (B04_20m / B08_20m / SCL_20m) for smaller files.
        """
        raw_res = raw_assets_cfg.get("band_resolution", 10)
        try:
            raw_res_int = int(raw_res)
        except Exception:
            raw_res_int = 10

        if self.is_dev:
            return 20
        return raw_res_int

    # ------------- Parallelism / OS tuning -------------

    def effective_max_download_workers(
        self, env: Optional[Mapping[str, str]] = None
    ) -> int:
        """
        Priority:
        1. THESS_MAX_DOWNLOAD_WORKERS env var
        2. dev: 4 workers, deep: 8 workers
        """
        env = env or os.environ
        override = env.get("THESS_MAX_DOWNLOAD_WORKERS")
        if override:
            try:
                return max(1, int(override))
            except ValueError:
                pass

        return 4 if self.is_dev else 8

    def effective_gdal_num_threads(
        self, env: Optional[Mapping[str, str]] = None
    ) -> Optional[str]:
        """
        Just returns THESS_GDAL_NUM_THREADS if set (string).
        You can plug this into GDAL/rasterio envs elsewhere.
        """
        env = env or os.environ
        return env.get("THESS_GDAL_NUM_THREADS")