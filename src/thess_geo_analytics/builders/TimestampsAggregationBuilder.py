from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any
from threading import Lock
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from tqdm import tqdm

from thess_geo_analytics.geo.TileAggregator import TileAggregator
from thess_geo_analytics.utils.RepoPaths import RepoPaths


@dataclass(frozen=True)
class TimestampsAggregationParams:
    """
    Parameters for timestamp-level aggregation.

    - max_workers:     >1 => parallel aggregation with ThreadPoolExecutor
                       1  => effectively sequential (unless debug=True forces it anyway)
    - merge_method:    passed to TileAggregator
    - resampling:      passed to TileAggregator
    - nodata:          passed to TileAggregator
    - bands:           which bands to aggregate per timestamp
    - debug:           when True, run sequentially and re-raise unexpected exceptions
                       (useful to see full tracebacks)
    """
    max_workers: int = 4
    merge_method: str = "first"
    resampling: str = "nearest"
    nodata: float = float("nan")
    bands: tuple[str, ...] = ("B04", "B08", "SCL")
    debug: bool = False


class TimestampsAggregationBuilder:
    """
    Aggregate per-timestamp Sentinel-2 tiles into one mosaic per band.

    Inputs:
      - outputs/tables/time_serie.csv
          must contain at least columns:
            * acq_datetime: real acquisition datetime (string)
            * tile_ids:     one or more scene IDs, separated by ; , or |

      - raw tiles in:
          RepoPaths.CACHE_S2 / <scene_id> / <band>.tif

    Outputs:
      - per-timestamp mosaics in:
          RepoPaths.DATA_RAW / "aggregated" / <timestamp_sanitized> / <band>.tif

      - logs in outputs/tables/:
          * timestamps_aggregation_status.csv   (per timestamp, coarse status)
          * timestamps_aggregation_summary.csv  (per timestamp, detailed)
          * timestamps_aggregation_band_report.csv (per band, detailed)
    """

    def __init__(self, params: TimestampsAggregationParams) -> None:
        self.params = params
        self.aggregator = TileAggregator(
            merge_method=params.merge_method,
            resampling=params.resampling,
            nodata=params.nodata,
            promote_to_float_if_needed=True,  
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run(self) -> List[Path]:
        ts_csv = RepoPaths.table("time_serie.csv")
        if not ts_csv.exists():
            raise FileNotFoundError(f"Missing time_serie.csv → {ts_csv}")

        df = pd.read_csv(ts_csv)
        if df.empty:
            print("[WARN] time_serie.csv is empty")
            return []

        required_cols = {"acq_datetime", "tile_ids"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(
                f"time_serie.csv must contain columns: {sorted(required_cols)} "
                f"(missing: {sorted(missing)})"
            )

        timestamps = sorted(df["acq_datetime"].unique())
        print(f"[INFO] Found {len(timestamps)} timestamps to aggregate")

        # shared logging structures
        status_rows: List[Dict[str, Any]] = []
        summary_rows: List[Dict[str, Any]] = []
        band_rows: List[Dict[str, Any]] = []
        log_lock = Lock()

        # --------------------------------------------------------------
        # Small helpers
        # --------------------------------------------------------------
        def _log_status(d: Dict[str, Any]) -> None:
            with log_lock:
                status_rows.append(d)

        def _log_summary(d: Dict[str, Any]) -> None:
            with log_lock:
                summary_rows.append(d)

        def _log_band(d: Dict[str, Any]) -> None:
            with log_lock:
                band_rows.append(d)

        def _sanitize_timestamp_for_folder(ts: str) -> str:
            # For folder names: avoid ':' and other problematic chars
            return str(ts).replace(":", "_").replace("/", "_")

        def _split_tile_ids(raw_id: Any) -> List[str]:
            """
            Normalize tile_ids into a list of *real* product IDs.

            Handles:
              - 'id1;id2;id3'
              - 'id1,id2,id3'
              - 'id1|id2|id3'
              - single scalar / already clean ids
            """
            if raw_id is None or (isinstance(raw_id, float) and pd.isna(raw_id)):
                return []

            if isinstance(raw_id, str):
                parts = re.split(r"[;,|]", raw_id)
                return [p.strip() for p in parts if p.strip()]

            # Fallback: treat as scalar and stringify
            return [str(raw_id)]

        # --------------------------------------------------------------
        # Core worker
        # --------------------------------------------------------------
        def _process_timestamp(ts: str) -> Path | None:
            """
            Process a single timestamp:
              - find all tile_ids in time_serie for this ts
              - gather existing B04/B08/SCL tiles from cache
              - if any missing → log + skip
              - else aggregate per band via TileAggregator
            """
            df_ts = df[df["acq_datetime"] == ts]

            # Collect all tile_ids for this timestamp, expanded and deduplicated
            scene_ids: list[str] = []
            for raw_id in df_ts["tile_ids"].tolist():
                scene_ids.extend(_split_tile_ids(raw_id))

            # Deduplicate while preserving order
            seen = set()
            scene_ids = [x for x in scene_ids if not (x in seen or seen.add(x))]

            # If somehow we have no scenes at all for this timestamp
            if not scene_ids:
                msg = "No tile_ids for this timestamp"
                _log_status(
                    {
                        "timestamp": ts,
                        "status": "failed_no_tiles",
                        "message": msg,
                    }
                )
                _log_summary(
                    {
                        "timestamp": ts,
                        "success": False,
                        "failed_bands": "",
                        "missing_files": "",
                        "error_message": msg,
                        "output_folder": "",
                    }
                )
                return None

            missing_files: list[tuple[str, str]] = []
            output_folder: Path | None = None

            try:
                band_inputs: Dict[str, List[Path]] = {b: [] for b in self.params.bands}

                # -----------------------------
                # Gather all input tiles per band
                # -----------------------------
                for scene_id in scene_ids:
                    scene_dir = RepoPaths.CACHE_S2 / scene_id

                    for band in self.params.bands:
                        tif = scene_dir / f"{band}.tif"
                        if not tif.exists():
                            missing_files.append((scene_id, str(tif)))
                            _log_band(
                                {
                                    "timestamp": ts,
                                    "band": band,
                                    "status": "missing_input",
                                    "input_files": "",
                                    "output_path": "",
                                    "message": f"Missing {tif}",
                                }
                            )
                            continue

                        band_inputs[band].append(tif)

                if missing_files:
                    msg = f"Missing {len(missing_files)} files"
                    _log_status(
                        {
                            "timestamp": ts,
                            "status": "failed_missing_inputs",
                            "message": msg,
                        }
                    )
                    _log_summary(
                        {
                            "timestamp": ts,
                            "success": False,
                            "failed_bands": "",
                            "missing_files": str(missing_files),
                            "error_message": msg,
                            "output_folder": "",
                        }
                    )
                    return None

                # -----------------------------
                # Prepare output folder
                # -----------------------------
                safe_ts = _sanitize_timestamp_for_folder(ts)
                output_folder = RepoPaths.DATA_RAW / "aggregated" / safe_ts
                output_folder.mkdir(parents=True, exist_ok=True)

                # -----------------------------
                # Aggregate per band
                # -----------------------------
                for band, paths in band_inputs.items():
                    out_tif = output_folder / f"{band}.tif"

                    # Extra guard: if for some reason this band has no inputs
                    if not paths:
                        raise ValueError(
                            f"No input files for band {band} at timestamp {ts}"
                        )

                    # 🚨 No try/except here – let TileAggregator errors propagate
                    self.aggregator.aggregate_band(
                        input_files=paths,
                        output_path=out_tif,
                    )

                    _log_band(
                        {
                            "timestamp": ts,
                            "band": band,
                            "status": "success",
                            "input_files": str(paths),
                            "output_path": str(out_tif),
                            "message": "OK",
                        }
                    )

                # If we got here, all bands succeeded
                _log_status(
                    {
                        "timestamp": ts,
                        "status": "success",
                        "message": "OK",
                    }
                )
                _log_summary(
                    {
                        "timestamp": ts,
                        "success": True,
                        "failed_bands": "",
                        "missing_files": "",
                        "error_message": "",
                        "output_folder": str(output_folder),
                    }
                )

                return output_folder

            except Exception as e:
                # In debug mode, do NOT swallow anything from TileAggregator:
                if self.params.debug:
                    raise

                msg = f"Unexpected error during aggregation for timestamp {ts}: {e}"
                _log_status(
                    {
                        "timestamp": ts,
                        "status": "exception",
                        "message": msg,
                    }
                )
                _log_summary(
                    {
                        "timestamp": ts,
                        "success": False,
                        "failed_bands": "",
                        "missing_files": "",
                        "error_message": msg,
                        "output_folder": str(output_folder) if output_folder else "",
                    }
                )
                return None

        # --------------------------------------------------------------
        # EXECUTION (sequential in debug / parallel otherwise)
        # --------------------------------------------------------------
        folders: List[Path] = []

        if self.params.debug or self.params.max_workers <= 1:
            print("[INFO] Running in DEBUG (sequential) mode")
            for ts in tqdm(timestamps, desc="Aggregating timestamps", unit="timestamp"):
                res = _process_timestamp(ts)
                if res:
                    folders.append(res)
        else:
            print(f"[INFO] Running in parallel with max_workers={self.params.max_workers}")
            with ThreadPoolExecutor(max_workers=self.params.max_workers) as ex:
                futs = {ex.submit(_process_timestamp, ts): ts for ts in timestamps}
                for fut in tqdm(
                    as_completed(futs),
                    total=len(futs),
                    desc="Aggregating timestamps",
                    unit="timestamp",
                ):
                    try:
                        res = fut.result()
                        if res:
                            folders.append(res)
                    except Exception as e:
                        # Should be rare since _process_timestamp already catches most errors
                        print(f"[WARN] Worker raised unhandled exception: {e}")

        # --------------------------------------------------------------
        # WRITE LOG FILES
        # --------------------------------------------------------------
        status_df = pd.DataFrame(status_rows)
        status_csv = RepoPaths.table("timestamps_aggregation_status.csv")
        status_df.to_csv(status_csv, index=False)

        summary_df = pd.DataFrame(summary_rows)
        summary_csv = RepoPaths.table("timestamps_aggregation_summary.csv")
        summary_df.to_csv(summary_csv, index=False)

        band_df = pd.DataFrame(band_rows)
        band_csv = RepoPaths.table("timestamps_aggregation_band_report.csv")
        band_df.to_csv(band_csv, index=False)

        print(f"[OK] Status written → {status_csv}")
        print(f"[OK] Summary written → {summary_csv}")
        print(f"[OK] Band report written → {band_csv}")
        if not summary_df.empty:
            print("[INFO] Summary counts:", summary_df["success"].value_counts().to_dict())

        return folders