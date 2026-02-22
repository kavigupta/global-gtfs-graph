"""Optional dependency: OSM/Overpass query. Provide query_to_geopandas for national_stations."""

try:
    from urbanstats.osm.query import query_to_geopandas
except ImportError:

    def query_to_geopandas(*args, **kwargs):
        raise NotImplementedError(
            "query_to_geopandas is required for national_stations. "
            "Install the urbanstats package or provide it via global_gtfs_graph.query."
        )
