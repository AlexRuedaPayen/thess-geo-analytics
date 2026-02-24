from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

from thess_geo_analytics.builders.TimestampsAggregationBuilder import (
    TimestampsAggregationBuilder,
    TimestampsAggregationParams,
)

class BuildAggregatedTimestampsPipeline:
    def run(self):
        params = TimestampsAggregationParams(max_workers=6)
        builder = TimestampsAggregationBuilder(params)
        out_folders = builder.run()
        
        print(f"[OK] Aggregated {len(out_folders)} timestamps")
        return out_folders