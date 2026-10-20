"""Compute real projected polygon centroids from the official TLC shapefile."""

import io
import zipfile

import requests

from src.extract.load_zones import fetch_zone_lookup_csv

SHAPEFILE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"


def fetch_authoritative_zones():
    # These libraries belong to extract jobs, never the API/UI runtime.
    import shapefile
    from pyproj import CRS, Transformer
    from shapely.geometry import shape

    response = requests.get(SHAPEFILE_URL, timeout=(10, 60))
    response.raise_for_status()
    if len(response.content) > 32 * 1024 * 1024:
        raise RuntimeError("Zone archive exceeds expected byte budget")
    metadata = {z["zone_id"]: z for z in fetch_zone_lookup_csv()}
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        stem = next(
            name[:-4]
            for name in archive.namelist()
            if name.endswith(".shp") and not name.startswith("__MACOSX/")
        )
        projection = CRS.from_wkt(archive.read(stem + ".prj").decode())
        transformer = Transformer.from_crs(
            projection, CRS.from_epsg(4326), always_xy=True
        )
        reader = shapefile.Reader(
            shp=io.BytesIO(archive.read(stem + ".shp")),
            shx=io.BytesIO(archive.read(stem + ".shx")),
            dbf=io.BytesIO(archive.read(stem + ".dbf")),
        )
        zones = []
        for item in reader.iterShapeRecords():
            record = item.record.as_dict()
            zone_id = int(record["LocationID"])
            if zone_id > 263 or zone_id not in metadata:
                continue
            centroid = shape(item.shape.__geo_interface__).centroid
            lon, lat = transformer.transform(centroid.x, centroid.y)
            if not (40 < lat < 42 and -75 < lon < -72):
                raise RuntimeError("Invalid transformed NYC zone centroid")
            zones.append(
                {**metadata[zone_id], "centroid_lat": lat, "centroid_lon": lon}
            )
    if len(zones) != 263:
        raise RuntimeError("Official shapefile must supply all 263 geographic zones")
    return zones
