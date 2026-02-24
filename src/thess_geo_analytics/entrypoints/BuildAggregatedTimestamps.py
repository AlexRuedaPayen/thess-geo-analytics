from __future__ import annotations

import argparse
from typing import Sequence

from thess_geo_analytics.builders.TimestampsAggregationBuilder import (
    TimestampsAggregationBuilder,
    TimestampsAggregationParams,
)
from thess_geo_analytics.core.pipeline_config import load_pipeline_config


def _as_bool01(x: str) -> bool:
    x = x.strip().lower()
    if x in {"1", "true", "yes", "y"}:
        return True
    if x in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"Expected boolean 0/1 or true/false, got: {x!r}")


def _parse_bands(bands: Sequence[str] | None) -> tuple[str, ...]:
    if not bands:
        return ("B04", "B08", "SCL")
    # allow comma-separated single arg or multiple args
    if len(bands) == 1 and ("," in bands[0] or ";" in bands[0] or "|" in bands[0]):
        import re
        parts = re.split(r"[;,|]", bands[0])
        return tuple(b.strip() for b in parts if b.strip())
    return tuple(bands)


def main() -> None:
    cfg = load_pipeline_config()
    ms = cfg.mode_settings

    # YAML block: timestamps_aggregation
    ta_cfg = cfg.raw.get("timestamps_aggregation", {}) or {}

    # base defaults from YAML
    yaml_max_workers = ta_cfg.get("max_workers")
    yaml_debug = ta_cfg.get("debug", False)
    yaml_merge_method = ta_cfg.get("merge_method", "first")
    yaml_resampling = ta_cfg.get("resampling", "nearest")
    yaml_nodata = ta_cfg.get("nodata", 0.0)  # 0.0 is safe for S2; can be NaN if you prefer
    yaml_bands = ta_cfg.get("bands", ["B04", "B08", "SCL"])

    # mode-aware default for workers if YAML didn't specify
    if yaml_max_workers is None:
        yaml_max_workers = ms.effective_max_download_workers()

    p = argparse.ArgumentParser(
        description="Aggregate per-timestamp Sentinel-2 tiles into mosaics "
                    "(one folder per acq_datetime under DATA_LAKE/data_raw/aggregated)."
    )

    p.add_argument(
        "--max-workers",
        type=int,
        default=yaml_max_workers,
        help=f"Max parallel timestamps to aggregate (default from YAML/mode: {yaml_max_workers}).",
    )
    p.add_argument(
        "--debug",
        default=str(yaml_debug),
        help="Debug mode (0/1, true/false). When true → sequential and re-raise errors.",
    )
    p.add_argument(
        "--merge-method",
        default=yaml_merge_method,
        help=f"Raster merge method (default from YAML: {yaml_merge_method!r}).",
    )
    p.add_argument(
        "--resampling",
        default=yaml_resampling,
        help=f"Resampling method (default from YAML: {yaml_resampling!r}).",
    )
    p.add_argument(
        "--nodata",
        type=float,
        default=yaml_nodata,
        help=f"Output nodata value (default from YAML: {yaml_nodata}). "
             "Use 0.0 for S2 uint16; use NaN with float output if desired.",
    )
    p.add_argument(
        "--bands",
        nargs="*",
        default=yaml_bands,
        help=f"Bands to aggregate per timestamp (default from YAML: {yaml_bands}). "
             "Example: --bands B04 B08 SCL",
    )

    args = p.parse_args()

    debug = _as_bool01(args.debug)
    bands = _parse_bands(args.bands)

    # Decide final max_workers + debug → parallel vs sequential
    max_workers = int(args.max_workers)
    if max_workers <= 0:
        max_workers = 1

    # Create params for the builder
    ta_params = TimestampsAggregationParams(
        max_workers=max_workers,
        merge_method=args.merge_method,
        resampling=args.resampling,
        nodata=args.nodata,
        bands=bands,
        debug=debug,
    )

    print(
        f"[INFO] Timestamps aggregation config -> "
        f"max_workers={max_workers}, debug={debug}, "
        f"merge_method={args.merge_method!r}, resampling={args.resampling!r}, "
        f"nodata={args.nodata}, bands={bands}"
    )

    builder = TimestampsAggregationBuilder(ta_params)
    out_folders = builder.run()

    print(f"[OK] Aggregated {len(out_folders)} timestamps")


if __name__ == "__main__":
    main()