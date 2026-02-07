from thess_geo_analytics.pipelines.ComputeMonthlyNdviStats import ComputeMonthlyNdviStats

class ComputeMonthlyNdviStatsRunner:
    def run(self, month: str):
        ComputeMonthlyNdviStats().run(month)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python -m thess_geo_analytics.pipelines.ComputeMonthlyNdviStatsRunner YYYY-MM")
    ComputeMonthlyNdviStatsRunner().run(sys.argv[1])
