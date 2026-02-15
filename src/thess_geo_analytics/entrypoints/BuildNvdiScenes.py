from __future__ import annotations

import argparse

from thess_geo_analytics.pipelines.BuildNdviScenePipeline import (
    BuildNdviScenePipeline,
    BuildNdviSceneParams,
)


def main() -> None:
    p = argparse.ArgumentParser(description="Build NDVI GeoTIFF + preview PNG for a single selected scene.")
    p.add_argument("scene_id", help="STAC item id (scene id), must exist in assets_manifest_selected.csv")
    p.add_argument(
        "--anchor-date",
        default=None,
        help="Optional YYYY-MM-DD used only for output naming (useful when running from time_serie.csv).",
    )
    args = p.parse_args()

    pipe = BuildNdviScenePipeline()
    out_tif, out_png = pipe.run(
        BuildNdviSceneParams(
            scene_id=args.scene_id,
            anchor_date=args.anchor_date,
        )
    )

    print("[OK] NDVI scene written:", out_tif)
    print("[OK] Preview written:   ", out_png)


if __name__ == "__main__":
    main()
