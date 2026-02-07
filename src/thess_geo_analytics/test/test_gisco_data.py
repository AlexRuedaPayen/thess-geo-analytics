import requests
from pathlib import Path

from thess_geo_analytics.utils.RepoPaths import RepoPaths
from thess_geo_analytics.services.NutsExtractor import NutsExtractor

url = "https://gisco-services.ec.europa.eu/distribution/v2/NUTS/GeoJSON/NUTS_RG_01M_2024_4326.geojson"
out =  RepoPaths.raw("NUTS_RG_01M_2024_4326.geojson")

out.parent.mkdir(parents=True, exist_ok=True)

r = requests.get(url, timeout=120)
r.raise_for_status()

out.write_bytes(r.content)
print("Saved:", out.resolve(), "bytes:", out.stat().st_size)
