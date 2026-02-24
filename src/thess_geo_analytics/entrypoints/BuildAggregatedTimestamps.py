from thess_geo_analytics.builders.TimestampsAggregationBuilder import (
    TimestampsAggregationBuilder,
    TimestampsAggregationParams,
)

def run():
    params = TimestampsAggregationParams(
        max_workers=1,   # sequential
        debug=True,      # see full tracebacks on unexpected errors
    )
    builder = TimestampsAggregationBuilder(params)
    out_folders = builder.run()

    print(f"[OK] Aggregated {len(out_folders)} timestamps")
    return out_folders
    

if __name__=='__main__':
    run()