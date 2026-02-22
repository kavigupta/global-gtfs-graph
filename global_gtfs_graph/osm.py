"""OSM/Overpass integration for station data."""

from . import query


def national_stations():
    query_str = """
    [out:json][timeout:25];
    area(id:3600148838)->.searchArea;
    (
    node["railway"="station"](area.searchArea);
    way["railway"="station"](area.searchArea);
    relation["railway"="station"](area.searchArea);
    );
    out body;
    >;
    out skel qt;
    """
    return query.query_to_geopandas(query_str, keep_tags=True)
