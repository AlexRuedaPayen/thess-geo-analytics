from __future__ import annotations

import argparse
from pathlib import Path

from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.pipelines.BuildNdviMonthlyCompositePipeline import (
    BuildNdviMonthlyCompositePipeline,
    BuildNdviMonthlyCompositeParams,
)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Build NDVI composites from time_serie.csv + assets_manifest_selected.csv "
                    "(monthly with optional quarterly fallback)."
    )

    p.add_argument(
        "--month",
        default=None,
        help="YYYY-MM. If provided, build only this month (no fallback).",
    )

    p.add_argument("--aoi", default="EL522_Thessaloniki.geojson", help="AOI geojson filename in /aoi")
    p.add_argument("--aoi-id", default="el522")

    p.add_argument(
        "--time-serie",
        default=str(RepoPaths.table("scenes_selected.csv")),
        help="Path to scenes_selected.csv (default: outputs/tables/scenes_selected.csv)",
    )
    p.add_argument(
        "--assets-manifest",
        default=str(RepoPaths.table("assets_manifest_selected.csv")),
        help="Path to assets_manifest_selected.csv (default: outputs/tables/assets_manifest_selected.csv)",
    )

    p.add_argument("--max-scenes", type=int, default=None, help="Max scenes used per period (month/quarter).")
    p.add_argument("--min-scenes-per-month", type=int, default=2, help="If month has fewer, fallback to quarter.")
    p.add_argument("--no-quarter-fallback", action="store_true", help="Disable quarterly fallback.")
    p.add_argument("--no-download", action="store_true", help="Do NOT download missing assets.")
    p.add_argument("--verbose", action="store_true")

    args = p.parse_args()

    aoi_path = RepoPaths.aoi(args.aoi)

    pipe = BuildNdviMonthlyCompositePipeline()

    outputs = pipe.run(
        BuildNdviMonthlyCompositeParams(
            aoi_path=aoi_path,
            aoi_id=args.aoi_id,
            month=args.month,
            time_serie_csv=Path(args.time_serie),
            assets_manifest_csv=Path(args.assets_manifest),
            max_scenes_per_period=args.max_scenes,
            download_missing=not args.no_download,
            verbose=args.verbose,
            min_scenes_per_month=args.min_scenes_per_month,
            fallback_to_quarterly=not args.no_quarter_fallback,
        )
    )

    for label, out_tif, out_png in outputs:
        print(f"[OK] {label} NDVI written: {out_tif}")
        print(f"[OK] {label} Preview:      {out_png}")


if __name__ == "__main__":
    main()
