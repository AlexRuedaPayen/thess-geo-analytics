from __future__ import annotations

from pathlib import Path
import pandas as pd

from thess_geo_analytics.RepoPaths import RepoPaths
from thess_geo_analytics.CdseStacItemService import CdseStacItemService
from thess_geo_analytics.StacAssetResolver import StacAssetResolver


class AssetsManifestBuilder:
 

    def __init__(
        self,
        month: str,  # "YYYY-MM"
        max_scenes: int = 10,
        collection: str = "sentinel-2-l2a",
    ) -> None:
        self.month = month
        self.max_scenes = max_scenes
        self.collection = collection
        self.item_service = CdseStacItemService()
        self.resolver = StacAssetResolver()

    def run(self) -> Path:
        scenes_csv = RepoPaths.table("scenes_catalog.csv")
        if not scenes_csv.exists():
            raise FileNotFoundError(f"Missing scenes catalog: {scenes_csv}")

        df = pd.read_csv(scenes_csv)
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        df = df.dropna(subset=["datetime"])

        df_month = df[df["datetime"].dt.strftime("%Y-%m") == self.month].copy()
        df_month = df_month.sort_values(["cloud_cover", "datetime"], ascending=[True, True]).head(self.max_scenes)

        rows = []
        for _, r in df_month.iterrows():
            item_id = r["id"]
            item_json = self.item_service.fetch_item(self.collection, item_id)
            hrefs = self.resolver.resolve_b04_b08_scl(item_json)

            rows.append(
                {
                    "scene_id": item_id,
                    "datetime": r["datetime"].isoformat(),
                    "cloud_cover": r.get("cloud_cover"),
                    "href_b04": hrefs.get("href_b04"),
                    "href_b08": hrefs.get("href_b08"),
                    "href_scl": hrefs.get("href_scl"),
                }
            )

        RepoPaths.TABLES.mkdir(parents=True, exist_ok=True)
        out_path = RepoPaths.table(f"assets_manifest_{self.month}.csv")

        out_df = pd.DataFrame(rows)
        out_df.to_csv(out_path, index=False)

        print(f"Assets manifest exported => {out_path}")
        print(f"Scenes in manifest: {len(out_df)}")

        if len(out_df) > 0:
            missing = out_df[["href_b04", "href_b08", "href_scl"]].isna().any(axis=1).sum()
            if missing > 0:
                print("WARNING: Some rows have missing hrefs (asset key mismatch).")
                print("Next step: inspect item_json['assets'].keys() and update StacAssetResolver.")

        return out_path
