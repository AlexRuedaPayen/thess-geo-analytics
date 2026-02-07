from __future__ import annotations

from pathlib import Path
import pandas as pd

from src.thess_geo_analytics.utils.RepoPaths import RepoPaths
from src.thess_geo_analytics.services.CdseStacItemService import CdseStacItemService
from src.thess_geo_analytics.services.StacAssetResolver import StacAssetResolver
from src.thess_geo_analytics.services.CdseTokenService import CdseTokenService
from src.thess_geo_analytics.services.CdseAssetDownloader import CdseAssetDownloader


class AssetsManifestBuilder:
    def __init__(
        self,
        month: str,  # "YYYY-MM"
        max_scenes: int = 10,
        collection: str = "sentinel-2-l2a",
        cache_root: Path = Path("DATA_LAKE/cache/s2"),
        download_n: int = 3,   # for acceptance test
        validate_rasterio: bool = True,
    ) -> None:
        self.month = month
        self.max_scenes = max_scenes
        self.collection = collection
        self.cache_root = cache_root
        self.download_n = download_n
        self.validate_rasterio = validate_rasterio

        self.item_service = CdseStacItemService()
        self.resolver = StacAssetResolver()

        self.token_service = CdseTokenService()
        self.downloader = CdseAssetDownloader(self.token_service)

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

            scene_dir = self.cache_root / item_id
            local_b04 = scene_dir / "B04.tif"
            local_b08 = scene_dir / "B08.tif"
            local_scl = scene_dir / "SCL.tif"

            rows.append(
                {
                    "scene_id": item_id,
                    "datetime": r["datetime"].isoformat(),
                    "cloud_cover": r.get("cloud_cover"),
                    "href_b04": hrefs.get("href_b04"),
                    "href_b08": hrefs.get("href_b08"),
                    "href_scl": hrefs.get("href_scl"),
                    "local_b04": str(local_b04),
                    "local_b08": str(local_b08),
                    "local_scl": str(local_scl),
                }
            )

        RepoPaths.TABLES.mkdir(parents=True, exist_ok=True)
        out_path = RepoPaths.table(f"assets_manifest_{self.month}.csv")

        out_df = pd.DataFrame(rows)
        out_df.to_csv(out_path, index=False)

        print(f"Assets manifest exported => {out_path}")
        print(f"Scenes in manifest: {len(out_df)}")

        self._download_and_validate(out_df)

        if len(out_df) > 0:
            missing = out_df[["href_b04", "href_b08", "href_scl"]].isna().any(axis=1).sum()
            if missing > 0:
                print("WARNING: Some rows have missing hrefs (asset key mismatch).")
                print("Next step: inspect item_json['assets'].keys() and update StacAssetResolver.")

        return out_path

    def _download_and_validate(self, out_df: pd.DataFrame) -> None:
        import rasterio

        n = min(self.download_n, len(out_df))
        for i in range(n):
            row = out_df.iloc[i]

            # skip if href missing
            if pd.isna(row["href_b04"]) or pd.isna(row["href_b08"]) or pd.isna(row["href_scl"]):
                print(f"Skipping download for {row['scene_id']} due to missing hrefs")
                continue

            p_b04 = Path(row["local_b04"])
            p_b08 = Path(row["local_b08"])
            p_scl = Path(row["local_scl"])

            if not p_b04.exists():
                self.downloader.download(row["href_b04"], p_b04)
            if not p_b08.exists():
                self.downloader.download(row["href_b08"], p_b08)
            if not p_scl.exists():
                self.downloader.download(row["href_scl"], p_scl)

            if self.validate_rasterio:
                for p in [p_b04, p_b08, p_scl]:
                    with rasterio.open(p) as ds:
                        _ = (ds.width, ds.height, ds.crs)
