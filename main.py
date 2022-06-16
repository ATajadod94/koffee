# formatting using black
import requests
import uvicorn
from fastapi import FastAPI, Query
from databases import Database
import pickle

from collections import namedtuple

car_info = namedtuple(
    "CarInfo", ["vin", "model", "make", "modelyear", "modelclass", "cache"]
)

app = FastAPI()


## VPIC functions
def vpic_extractor(vin, r):
    return car_info(
        vin,
        r.json()["Results"][0]["Model"],
        r.json()["Results"][0]["Make"],
        r.json()["Results"][0]["ModelYear"],
        r.json()["Results"][0]["BodyClass"],
        False,  # its not cached if you're in here
    )


def external_vpic(vin: str = 17):
    post_fields = {"format": vpic_config.format, "data": vin}
    r = requests.post(vpic_config.url, data=post_fields)
    return vpic_config.extract_results(vin, r)


class vpic_config:
    url = "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/"
    format = "json"
    extract_results = vpic_extractor


## Cache functions


def parse_cache_results(vin, r):
    return car_info(
        vin,
        r[1],
        r[2],
        r[3],
        r[4],
        True,  # its cached if you are here
    )


async def cache_insert(record: car_info):
    insert_query = "insert into cache (vin, model, make, year, class) values ('{}', '{}', '{}', '{}', '{}')".format(
        record.vin, record.model, record.make, record.modelyear, record.modelclass
    )
    await cache_adapter.database.execute(insert_query)
    return True


async def get_cache_singlerecord(vin: str = 17):
    # usually like to keep my sql seperate but a lot of over eng for the given task
    query = "SELECT * FROM cache WHERE vin='{}'".format(vin)
    res = await cache_adapter.database.fetch_one(query=query)

    if res:
        return cache_adapter.parse_results(vin, res)

    return False


async def get_cache_records():
    # usually like to keep my sql seperate but a lot of over eng for the given task
    query = "SELECT * FROM cache"
    res = await cache_adapter.database.fetch_all(query=query)
    # res = cache_adapter.parse_results(res)
    if res:
        results = [cache_adapter.parse_results(x[0], x) for x in res]
        return results
    else:
        return False


async def cache_remove(vin: str = 17):
    query = "DELETE FROM cache WHERE vin ='{}'".format(vin)
    return await cache_adapter.database.execute(query=query)


class cache_adapter:
    database = Database(
        "sqlite:///koffee.sqllite"
    )  # should be in a cache_config but keeping scope in mind
    parse_results = parse_cache_results
    insert_results = cache_insert
    remove_results = cache_remove
    get_results = get_cache_records


## End points


@app.get("/lookup")
async def lookup(vin: str = Query(default=None, min_length=17, max_length=17)):
    cache_res = await get_cache_singlerecord(vin)
    if cache_res:
        return cache_res
    else:
        res = external_vpic(vin)
        await cache_adapter.insert_results(res)
        return res


@app.get("/remove")
async def remove(vin: str = Query(default=None, min_length=17, max_length=17)):
    res = await cache_adapter.remove_results(vin)
    return vin, res > 0  # false if nothing was deleted


@app.get("/export")
async def export():
    records = await cache_adapter.get_results()
    with open("response.pickle", "wb") as f:
        for record in records:
            pickle.dump(record._asdict(), f)

    return "Success"
