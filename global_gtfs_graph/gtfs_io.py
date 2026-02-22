"""Low-level GTFS file reading utilities."""

import pandas as pd


def pull_file_from_gtfs(gtfs, filename):
    matching_keys = [key for key in gtfs if key.split("/")[-1] == filename]
    if len(matching_keys) > 1:
        raise ValueError(f"Multiple matching files for {filename}: {matching_keys}")
    if len(matching_keys) == 0:
        return None
    if gtfs[matching_keys[0]] is None:
        return None
    pulled = gtfs[matching_keys[0]].copy()
    pulled.columns = [c.strip() for c in pulled.columns]
    return pulled


def read_try_multiple_encodings(file):
    try:
        try:
            return pd.read_csv(file())
        except UnicodeDecodeError:
            return pd.read_csv(file(), encoding="latin1")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except pd.errors.ParserError:
        return None
