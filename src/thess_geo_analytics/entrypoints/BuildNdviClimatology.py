from __future__ import annotations

import argparse
from typing import Sequence

from thess_geo_analytics.core.pipeline_config import load_pipeline_config
from thess_geo_analytics.pipelines.BuildNdviClimatologyPipeline import (
    BuildNdviClimatologyParams,
    BuildNdviClimatologyPipeline,
)
from thess_geo_analytics.utils.log_parameters import log_parameters


PARAMETER_DOCS = {
    "aoi_id": "AOI identifier (comes from pipeline.thess.yaml).",
    "in_stats_csv": (
        "Path to per-period NDVI stats table (ndvi_period_stats.csv). "
        "If missing and fallback from cogs is enabled, per-period stats "
        "are computed directly from NDVI composites in outputs/cogs."
    ),
    "allow_fallback_from_cogs": (
        "If true, and ndvi_period_stats.csv is missing, derive the necessary "
        "per-period NDVI statistics directly from ndvi_<period>_<aoi_id>.tif "
        "in outputs/cogs."
    ),
    "out_csv": (
        "Output NDVI climatology table (legacy name: nvdi_climatology.csv) "
        "in outputs/tables."
    ),
    "out_csv_canonical": (
        "Canonical NDVI climatology table (ndvi_climatology.csv) "
        "in outputs/tables."
    ),
    "out_fig": (
        "PNG figure with the seasonal NDVI curve "
        "(outputs/figures/ndvi_climatology.png)."
    ),
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """
    Runtime overrides.

    AOI / region are config-driven; we only expose a small switch
    that controls whether we are allowed to fall back to reading NDVI
    composites when the stats CSV is missing.
    """
    p = argparse.ArgumentParser(
        description="Build NDVI climatology (monthly or quarterly, config-driven)."
    )

    p.add_argument(
        "--csv-only",
        action="store_true",
        help=(
            "Require ndvi_period_stats.csv to exist. "
            "If set, the pipeline will NOT derive stats from NDVI cogs "
            "and will fail instead if the CSV is missing."
        ),
    )

    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    # 1) Runtime args
    args = parse_args(argv)

    # 2) Load pipeline config (AOI / region / mode, etc.)
    cfg = load_pipeline_config()
    aoi_id = cfg.aoi_id

    # 3) Build params (mostly config-driven)
    params = BuildNdviClimatologyParams(
        aoi_id=aoi_id,
        # If user passes --csv-only, we disable fallback from cogs.
        allow_fallback_from_cogs=not args.csv_only,
    )

    # Extra context for logging
    extra = {
        "mode": cfg.mode,
        "region": cfg.region_name,
        "aoi_id": cfg.aoi_id,
    }

    log_parameters(
        "ndvi_climatology",
        params,
        extra=extra,
        docs=PARAMETER_DOCS,
    )

    # 4) Run pipeline
    pipe = BuildNdviClimatologyPipeline()
    out_csv, out_fig = pipe.run(params)

    print("\n=== OUTPUTS (NDVI climatology) ===")
    print(f"[OK] Climatology table → {out_csv}")
    print(f"[OK] Climatology plot  → {out_fig}")


if __name__ == "__main__":
    main()