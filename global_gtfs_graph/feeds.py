"""GTFS feed listing and download from Transitland/GitHub."""

import io
import json
import os
import zipfile

import pandas as pd
import requests
from permacache import permacache

from . import gtfs_io

repo = "transitland/transitland-atlas"
hash = "821069d1a80ee041e29d86ee093c5b71ddcf0da4"


@permacache("urbanstats/osm/trains/gtfs_list_2", multiprocess_safe=True)
def gtfs_list():
    url_api = f"https://api.github.com/repos/{repo}/git/trees/{hash}?recursive=1"
    tree = requests.get(url_api).json()["tree"]
    feeds = [x["path"] for x in tree if x["path"].startswith("feeds/")]
    return feeds


@permacache("urbanstats/osm/trains/read_gtfs_spec_2", multiprocess_safe=True)
def read_gtfs_spec(feed_path):
    url = f"https://raw.githubusercontent.com/{repo}/{hash}/{feed_path}"
    return json.loads(requests.get(url).content)


def api_key():
    with open(os.path.expanduser("~/.transitland")) as f:
        api_key = f.read().strip()
    return api_key


@permacache(
    "urbanstats/osm/trains/read_gtfs_from_feed_id_raw_5", multiprocess_safe=True
)
def read_gtfs_from_feed_id_raw(feed_id):
    try:
        result = requests.get(
            f"https://transit.land/api/v2/rest/feeds/{feed_id}/download_latest_feed_version",
            params=dict(api_key=api_key()),
        )
        if result.status_code != 200:
            return {
                "status": "failure",
                "reason": f"status code {result.status_code}; content: {result.content}",
            }
        return {"status": "success", "content": result.content}
    except requests.exceptions.Timeout:
        return {"status": "failure", "reason": "timeout"}
    except requests.exceptions.ConnectionError:
        return {"status": "failure", "reason": "connection error"}
    except requests.exceptions.ContentDecodingError:
        return {"status": "failure", "reason": "content decoding error"}
    except requests.exceptions.ChunkedEncodingError:
        return {"status": "failure", "reason": "chunked encoding error"}


@permacache("urbanstats/osm/trains/read_gtfs_from_feed_id_4", multiprocess_safe=True)
def read_gtfs_from_feed_id(feed_id):
    res = read_gtfs_from_feed_id_raw(feed_id)
    if res["status"] == "failure":
        return res
    zip_buf = io.BytesIO(res["content"])
    try:
        zip_file = zipfile.ZipFile(zip_buf)
    except zipfile.BadZipFile as e:
        return {"status": "failure", "reason": "bad zip file"}
    return {
        "status": "success",
        "content": {
            name: gtfs_io.read_try_multiple_encodings(lambda: zip_file.open(name))
            for name in zip_file.namelist()
        },
    }


def all_gtfs_info():
    import tqdm.auto as tqdm

    urls = gtfs_list()
    for url in tqdm.tqdm(urls):
        spec = read_gtfs_spec(url)
        for feed in spec["feeds"]:
            yield dict(
                feed=feed,
                gtfs_result=lambda feed=feed: read_gtfs_from_feed_id(feed["id"]),
            )


@permacache("urbanstats/osm/trains/all_failures")
def all_failures():
    bad_feeds = []
    for res in all_gtfs_info():
        gtfs = res["gtfs_result"]()
        if gtfs["status"] == "failure":
            bad_feeds.append((res["feed"], gtfs["reason"]))
    return bad_feeds
