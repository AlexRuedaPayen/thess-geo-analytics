# 01 — Extract AOI

## Overview

An Area of Interest is a reguional or national geopspatial dataset (e.g NUU)

In this first stage, we are going to fetch an area of interest, which is a geometric delimitation on the e4326 earth porjection system, of an Area Of Interest (AOI)...

The goal is to focus on a small subregion (here we will take NUTS - level 2 : Munipcipality zone), where geography is varied : cityscape, low altitude hills and mountains (Mount Choriatis), sea (Thesssaloniki's bay within Aegan Sea), lakes (Limni Koronia), agriculture crops (Axios-Loudias-Aliakomonas) etc. 

We will in a first time just delimitate this region


---

## Input


---

## Output


---

## Choices

None

---

## Process
`

### 1. GISCO Service


We will use these variables stored in core.constant

```
    GISCO_NUTS_URL,
    HTTP_TIMEOUT,
    DEFAULT_NUTS_FILENAME,

```
to target the right  API and store the GISCO file we are interested in (with coordinates e4326). Then we will use these variables from core.parameters


```
    DATA_RAW_DIR,
    NUTS_LOCAL_PATH,
    AUTO_DOWNLOAD_GISCO,
---

to store the GISCO data file and the final file where the region has been filtered at the right place.

## How to run it

* Modify inside **config\pipeline.thess.yaml** 

```
region:
  name: "Thessaloniki"

aoi:
  id: "el522"
  file: "EL522_Thessaloniki.geojson"
```

to choose the region that will be analyzed

* The, run

```
make extract-aoi
```


--

## Dependencies 

None

## Limitations

* Doesn't eliminate low interest areas : lakes, river, urbanscape etc. which means that some pixel will be lost
* No geometry checker yet, will be implemented in a feature version