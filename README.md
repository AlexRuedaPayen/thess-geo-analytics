<h1>Thess Geo Analytics</h1>

<p>
Sentinel-2 raster processing pipeline for NDVI time-series analysis and pixel-level feature extraction.
</p>

<p>
Developed by <b>Alexandre Rueda Payen</b>.
</p>

<p>
This repository implements a modular pipeline that transforms Sentinel-2 imagery into:
</p>

<ul>
  <li>NDVI composites</li>
  <li>NDVI climatology rasters</li>
  <li>NDVI anomaly maps</li>
  <li>pixel-level temporal features for machine learning</li>
</ul>

<p>
The project focuses on <b>geospatial raster engineering</b> and <b>Earth Observation data pipelines</b>.
</p>

<p>
Detailed technical explanations, design notes, and implementation comments are documented in the <b>Wiki</b>.
</p>

<hr>

<h2>Key Features</h2>

<ul>
  <li>Block-wise raster processing for large datasets</li>
  <li>Cloud-Optimized GeoTIFF style outputs</li>
  <li>Temporal NDVI compositing and anomaly detection</li>
  <li>Pixel-level feature engineering for downstream ML</li>
  <li>Modular pipeline architecture</li>
  <li>Config-driven execution</li>
</ul>

<hr>

<h2>Pipeline Overview</h2>

<pre>
Sentinel-2 scenes
      ↓
tile aggregation
      ↓
NDVI composites
      ↓
climatology rasters
      ↓
NDVI anomaly maps
      ↓
pixel time-series feature extraction
</pre>

<p>
Outputs include:
</p>

<ul>
  <li>analysis-ready NDVI rasters</li>
  <li>anomaly rasters</li>
  <li>climatology rasters</li>
  <li>pixel-level feature stacks</li>
</ul>

<p>
<!-- TODO -->
Add PNG previews here:
</p>

<ul>
  <li>NDVI composite example</li>
  <li>NDVI anomaly example</li>
  <li>Pixel feature raster example</li>
</ul>

<hr>

<h2>Repository Architecture</h2>

<pre>
thess-geo-analytics/

config/
    pipeline.thess.yaml
    Main pipeline configuration

src/thess_geo_analytics/

    entrypoints/
        CLI entry scripts

    pipelines/
        high-level workflow orchestration

    builders/
        heavy processing modules

    geo/
        raster and EO processing logic

    services/
        external IO and retrieval logic

    utils/
        paths, logging, helpers

outputs/
    generated rasters, tables, figures

tests/
    unit and integration tests
</pre>

<hr>

<h3>Architecture Rationale</h3>

<p>
The codebase separates responsibilities to keep the pipeline easier to maintain, test, and extend.
</p>

<h4>geo</h4>

<p>
Contains the <b>core geospatial processing logic</b>.
</p>

<ul>
  <li>NDVI computation</li>
  <li>cloud masking</li>
  <li>AOI masking</li>
  <li>window / tile processing</li>
  <li>climatology and anomaly raster logic</li>
  <li>pixel feature extraction</li>
</ul>

<p>
These modules should stay as close as possible to the raster math and geospatial transformations themselves.
</p>

<h4>builders</h4>

<p>
Builders perform the <b>heavy transformations</b> on datasets.
</p>

<ul>
  <li>build NDVI composites</li>
  <li>build climatology rasters</li>
  <li>build anomaly rasters</li>
  <li>build feature rasters</li>
</ul>

<p>
They are usually the modules that read many rasters, loop over windows, and write outputs.
</p>

<h4>pipelines</h4>

<p>
Pipelines orchestrate multiple builders and define the <b>high-level processing flow</b>.
</p>

<p>
They express the order of operations, but they should not contain the low-level raster algorithms themselves.
</p>

<h4>entrypoints</h4>

<p>
Entrypoints expose the code as <b>runnable commands</b>.
</p>

<p>
They usually:
</p>

<ul>
  <li>load the configuration</li>
  <li>instantiate the appropriate pipeline</li>
  <li>run it</li>
</ul>

<p>
Entrypoints should remain thin and readable.
</p>

<h4>services</h4>

<p>
Services handle <b>external systems and IO</b>.
</p>

<ul>
  <li>catalog access</li>
  <li>scene retrieval</li>
  <li>asset downloading</li>
  <li>other external interactions</li>
</ul>

<p>
This avoids mixing external system logic with raster computation.
</p>

<hr>

<h2>Parameter Modularity</h2>

<p>
Pipeline parameters are defined using <b>dedicated parameter structures</b> (typically dataclasses).
</p>

<p>
Examples:
</p>

<pre>
BuildNdviAnomalyMapsParams
BuildPixelFeaturesParams
BuildNdviClimatologyParams
</pre>

<p>
This design keeps parameters explicit and local to each module.
</p>

<p>
Why this helps:
</p>

<ul>
  <li>clear parameter ownership</li>
  <li>better type safety</li>
  <li>less hidden state</li>
  <li>easier testing</li>
  <li>easier refactoring</li>
</ul>

<p>
User-facing configuration is stored in:
</p>

<pre>
config/pipeline.thess.yaml
</pre>

<p>
The idea is to change pipeline behaviour through configuration without rewriting processing code.
</p>

<p>
<!-- TODO -->
Add links here to Wiki pages documenting the main parameter groups.
</p>

<hr>

<h2>Requirements</h2>

<ul>
  <li>Python 3.11+</li>
  <li>Rasterio</li>
  <li>NumPy</li>
  <li>Pandas</li>
  <li>GDAL-compatible environment</li>
  <li>Make (optional but recommended)</li>
</ul>

<p>
Optional:
</p>

<ul>
  <li>Copernicus Data Space credentials for scene retrieval</li>
</ul>

<hr>

<h2>Installation</h2>

<p>
Clone the repository:
</p>

<pre>
git clone https://github.com/&lt;your-repo&gt;/thess-geo-analytics.git
cd thess-geo-analytics
</pre>

<p>
Create a virtual environment:
</p>

<pre>
python -m venv .venv
source .venv/bin/activate
</pre>

<p>
Windows:
</p>

<pre>
.venv\Scripts\activate
</pre>

<p>
Install dependencies:
</p>

<pre>
pip install -r requirements.txt
</pre>

<p>
<!-- TODO -->
If you use a pyproject.toml / editable install, add the command here.
Example:
</p>

<pre>
pip install -e .
</pre>

<hr>

<h2>Configuration</h2>

<p>
Pipeline behaviour is controlled through:
</p>

<pre>
config/pipeline.thess.yaml
</pre>

<p>
Example:
</p>

<pre>
region: Thessaloniki
aoi_id: el522

pipeline:
  date_start: "2021-01-01"

raster:
  resolution: 10
</pre>

<p>
Typical configuration controls:
</p>

<ul>
  <li>AOI / region</li>
  <li>time range</li>
  <li>raster resolution</li>
  <li>scene selection rules</li>
  <li>NDVI composite strategy</li>
  <li>anomaly and feature extraction settings</li>
</ul>

<p>
<!-- TODO -->
Add a short example of your real config or link the full config section in the Wiki.
</p>

<hr>

<h2>Build Instructions</h2>

<p>
If running locally, the usual build/setup process is:
</p>

<pre>
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
</pre>

<p>
If the package needs to be installed in editable mode:
</p>

<pre>
pip install -e .
</pre>

<p>
If you use tests before running the pipeline:
</p>

<pre>
pytest
</pre>

<p>
<!-- TODO -->
If there is a preferred command for validating the environment, add it here.
</p>

<hr>

<h2>Running the Pipeline</h2>

<p>
The repository uses a <b>Makefile</b> to run pipeline steps.
</p>

<p>
Available commands:
</p>

<pre>
make extract-aoi
make scene-catalog
make assets-manifest
make timestamps-aggregation
make ndvi-composites
make anomalies
make pixel-features
</pre>

<p>
Run the full pipeline:
</p>

<pre>
make full
</pre>

<p>
<!-- TODO -->
Adjust these Make targets so they match the real Makefile exactly.
</p>

<hr>

<h2>Running Individual Modules</h2>

<p>
Example: build NDVI composites
</p>

<pre>
python -m thess_geo_analytics.entrypoints.BuildNdviComposites
</pre>

<p>
Example: build anomaly maps
</p>

<pre>
python -m thess_geo_analytics.entrypoints.BuildNdviAnomalyMaps
</pre>

<p>
Example: build pixel features
</p>

<pre>
python -m thess_geo_analytics.entrypoints.BuildPixelFeatures
</pre>

<p>
Each module reads parameters from the YAML configuration or from its dedicated parameter structure.
</p>

<p>
<!-- TODO -->
Replace module names if your entrypoint filenames differ.
</p>

<hr>

<h2>Docker Build and Execution</h2>

<p>
The project can also be executed inside Docker to avoid dependency issues, especially around GDAL / Rasterio.
</p>

<p>
Outputs should be written outside the container using a <b>volume mount</b>, so generated rasters remain accessible on the host machine.
</p>

<hr>

<h3>1. Build the Docker image</h3>

<pre>
docker build -t thess-geo-analytics .
</pre>

<p>
This builds the container image from the repository root.
</p>

<p>
<!-- TODO -->
If your Dockerfile has a different name or location, update this command.
</p>

<hr>

<h2>Docker Execution</h2>

<p>
The pipeline can be executed inside a Docker container.  
This avoids dependency issues related to GDAL / Rasterio and ensures a reproducible environment.
</p>

<p>
Outputs are written outside the container using <b>volume mounts</b>, so generated rasters remain accessible on the host machine.
</p>

<hr>

<h3>1. Build the Docker image</h3>

<pre>
docker build -t thess-geo-analytics:0.3.1 .
</pre>

<p>
Run this command from the repository root.
</p>

<hr>

<h3>2. Run the container</h3>

<p>
Example execution command (PowerShell):
</p>

<pre>
docker run -it --rm `
  -v "C:\Users\alexr\OneDrive\Documents\thess-geo-analytics-0.3.1\DATA_LAKE:/data_lake" `
  -v "C:\Users\alexr\OneDrive\Documents\thess-geo-analytics-0.3.1\aoi:/app/aoi" `
  -v "C:\Users\alexr\OneDrive\Documents\thess-geo-analytics-0.3.1\outputs:/app/outputs" `
  -v "C:\Users\alexr\OneDrive\Documents\thess-geo-analytics-0.3.1\config:/app/config:ro" `
  -v "C:\Users\alexr\OneDrive\Desktop\thess-geo-analytics\.env:/app/.env:ro" `
  --env-file "C:\Users\alexr\OneDrive\Desktop\thess-geo-analytics\.env" `
  -e DATA_LAKE=/data_lake `
  -e PIPELINE_CONFIG=config/pipeline.thess.yaml `
  -e THESS_GEO_ROOT=/app `
  thess-geo-analytics:0.3.1
</pre>

<hr>

<h3>3. Volume Mounts</h3>

<p>
The container relies on several mounted directories:
</p>

<ul>

<li>
<b>DATA_LAKE</b>  
<pre>
C:\...\DATA_LAKE:/data_lake
</pre>
Raw satellite data and intermediate rasters.
</li>

<li>
<b>AOI folder</b>  
<pre>
C:\...\aoi:/app/aoi
</pre>
Contains AOI geometries.
</li>

<li>
<b>Outputs</b>  
<pre>
C:\...\outputs:/app/outputs
</pre>
All generated rasters and tables are written here.
</li>

<li>
<b>Configuration</b>  
<pre>
C:\...\config:/app/config:ro
</pre>
Pipeline configuration file.
</li>

<li>
<b>.env file</b>  
<pre>
C:\...\ .env:/app/.env:ro
</pre>
Credentials for satellite data services.
</li>

</ul>

<hr>

<h3>4. Required Environment Variables</h3>

<p>
The pipeline requires credentials for accessing Sentinel-2 data providers.
</p>

<p>
These must be stored in a <code>.env</code> file.
</p>

<p>
Example:
</p>

<pre>
SH_CLIENT_ID=xxxx
SH_CLIENT_SECRET=xxxx

CDSE_USERNAME=xxxx
CDSE_PASSWORD=xxxx
</pre>

<p>
Where:
</p>

<ul>

<li>
<b>SH_CLIENT_ID / SH_CLIENT_SECRET</b>  
Credentials for <b>Sentinel Hub</b>.
</li>

<li>
<b>CDSE_USERNAME / CDSE_PASSWORD</b>  
Credentials for <b>Copernicus Data Space Ecosystem</b>.
</li>

</ul>

<p>
The container reads these values through:
</p>

<pre>
--env-file path/to/.env
</pre>

<hr>

<h3>5. Notes</h3>

<ul>

<li>
The container environment ensures compatibility with GDAL and Rasterio.
</li>

<li>
All raster outputs are written to the mounted <code>outputs/</code> directory.
</li>

<li>
Configuration changes can be applied by editing <code>config/pipeline.thess.yaml</code> without rebuilding the image.
</li>

</ul>

<hr>

<h3>Credentials Access</h3>

<p>
If you do not have Copernicus or Sentinel Hub credentials and need them for testing the pipeline,
you may contact:
</p>

<p>
<b>alexruedapayen@gmail.com</b>
</p>

<p>
Temporary access may be provided for demonstration or evaluation purposes.
</p>

<hr>

<h2>Pipeline Stages</h2>

<h3>1. AOI Extraction</h3>

<p>
Input:
</p>

<pre>
NUTS boundaries / AOI definition
</pre>

<p>
Output:
</p>

<pre>
AOI GeoJSON
</pre>

<p>
The AOI defines the spatial mask used for all later raster processing.
</p>

<p>
See the Wiki for details.
</p>

<h3>2. Scene Catalog</h3>

<p>
Builds a catalog of Sentinel-2 scenes intersecting the AOI.
</p>

<p>
Typical filters include:
</p>

<ul>
  <li>cloud cover</li>
  <li>AOI coverage</li>
  <li>temporal sampling</li>
</ul>

<p>
Outputs:
</p>

<pre>
scenes_s2_all.csv
scenes_selected.csv
</pre>

<h3>3. Asset Manifest</h3>

<p>
Determines which raster assets are required for selected scenes.
</p>

<p>
Typical assets:
</p>

<ul>
  <li>B04 (RED)</li>
  <li>B08 (NIR)</li>
  <li>SCL (optional cloud mask)</li>
</ul>

<p>
Output:
</p>

<pre>
assets_manifest_selected.csv
</pre>

<h3>4. Timestamp Aggregation</h3>

<p>
Groups tiles by timestamp and mosaics them into AOI-scale rasters.
</p>

<p>
Output example:
</p>

<pre>
outputs/cogs/aggregated/&lt;timestamp&gt;/
</pre>

<h3>5. NDVI Composites</h3>

<p>
Computes NDVI from RED and NIR bands.
</p>

<pre>
NDVI = (NIR - RED) / (NIR + RED)
</pre>

<p>
Composite strategies include:
</p>

<ul>
  <li>monthly composites</li>
  <li>quarterly fallback when monthly coverage is too sparse</li>
</ul>

<p>
Outputs:
</p>

<pre>
ndvi_YYYY-MM_&lt;aoi&gt;.tif
ndvi_YYYY-Qn_&lt;aoi&gt;.tif
</pre>

<h3>6. Climatology</h3>

<p>
Builds per-pixel climatology rasters from historical NDVI composites.
</p>

<p>
Example:
</p>

<pre>
ndvi_climatology_median_05_el522.tif
</pre>

<p>
Meaning: median NDVI for May across years.
</p>

<h3>7. NDVI Anomaly Maps</h3>

<p>
Computes anomalies relative to climatology:
</p>

<pre>
NDVI anomaly = NDVI(period) - climatology(period_of_year)
</pre>

<p>
Outputs:
</p>

<pre>
ndvi_anomaly_YYYY-MM_&lt;aoi&gt;.tif
ndvi_anomaly_YYYY-Qn_&lt;aoi&gt;.tif
</pre>

<p>
PNG previews can also be generated.
</p>

<h3>8. Pixel Feature Extraction</h3>

<p>
Converts the NDVI anomaly time series into a multi-band feature raster.
</p>

<p>
Output example:
</p>

<pre>
pixel_features_7d_&lt;aoi&gt;.tif
</pre>

<p>
Typical output shape:
</p>

<pre>
(height, width, 7)
</pre>

<p>
These features summarize the temporal behaviour of each pixel and can be used later for ML workflows.
</p>

<hr>

<h2>Outputs</h2>

<p>
Outputs are written under:
</p>

<pre>
outputs/

    cogs/
        NDVI rasters
        anomaly rasters
        climatology rasters
        feature rasters

    tables/
        diagnostics
        summary statistics

    figures/
        PNG previews
</pre>

<p>
Example outputs:
</p>

<pre>
ndvi_2023-05_el522.tif
ndvi_anomaly_2023-05_el522.tif
pixel_features_7d_el522.tif
</pre>

<p>
<!-- TODO -->
Add PNG preview images here once available.
</p>

<hr>

<h2>Testing</h2>

<p>
The repository includes tests for core logic and pipeline behaviour.
</p>

<p>
Run tests with:
</p>

<pre>
pytest
</pre>

<p>
<!-- TODO -->
If you have a dedicated integration test command, add it here.
</p>

<hr>

<h2>Documentation</h2>

<p>
Most technical explanations are documented in the <b>Wiki</b>.
</p>

<p>
Suggested Wiki sections:
</p>

<ul>
  <li>architecture overview</li>
  <li>configuration guide</li>
  <li>NDVI compositing</li>
  <li>climatology and anomalies</li>
  <li>pixel feature engineering</li>
  <li>known limitations</li>
</ul>

<p>
<!-- TODO -->
Add actual Wiki links here.
</p>

<hr>

<h2>Known Limitations</h2>

<ul>
  <li>some temporal features still assume simplified time spacing</li>
  <li>some modules remain prototype-like and can still be cleaned further</li>
  <li>feature definitions are intentionally simple at this stage</li>
</ul>

<p>
More detailed notes are available in the Wiki.
</p>

<hr>

<h2>Purpose</h2>

<p>
This repository was developed as an independent project to demonstrate:
</p>

<ul>
  <li>Earth Observation raster engineering</li>
  <li>Sentinel-2 processing pipelines</li>
  <li>geospatial data engineering</li>
  <li>reproducible EO workflows</li>
</ul>

<p>
The goal is to produce analysis-ready EO datasets suitable for downstream machine learning tasks.
In future version we will implement pixel-wise clustering to determinate for instance disaster zones, sensitive zones, urban zones, re-vegetalized zone etc.
</p>
