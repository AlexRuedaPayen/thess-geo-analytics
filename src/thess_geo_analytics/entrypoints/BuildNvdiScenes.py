from __future__ import annotations

import argparse

from thess_geo_analytics.pipelines import (
    BuildNdviScenePipeline,
    BuildNdviSceneParams,
)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("month", help="YYYY-MM")
    p.add_argument("scene_id", help="STAC item id (scene id)")
    args = p.parse_args()

    pipe = BuildNdviScenePipeline()
    out_tif, out_png = pipe.run(BuildNdviSceneParams(month=args.month, scene_id=args.scene_id))

    print("[OK] NDVI scene written:", out_tif)
    print("[OK] Preview written:   ", out_png)


if __name__ == "__main__":
    main()
