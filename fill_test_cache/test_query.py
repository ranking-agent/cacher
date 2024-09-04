from datetime import datetime as dt
import httpx
import json
import logging
import os
from tqdm import tqdm

now = dt.now().strftime('%Y_%m_%d_%H_%M_%S')
logging.basicConfig(filename=f"{now}.log", level=logging.INFO, format="[%(asctime)s: %(levelname)s/%(name)s]: %(message)s")

aragorn_url = "https://aragorn.ci.transltr.io/aragorn/query"


def single_lookup(query):
    """Run a single query lookup synchronously."""
    start_time = dt.now()
    # query["bypass_cache"] = True
    query["parameters"] = {
        # "overwrite_cache": True,
        "timeout_seconds": 3600,
        "kp_timeout": 300,
    }
    response = {}
    status = 410
    try:
        with httpx.Client(timeout=3700) as client:
            res = client.post(
                aragorn_url,
                json=query,
            )
            status = res.status_code
            res.raise_for_status()
            response = res.json()
    except Exception as e:
        logging.error(e)
        num_results = 0
        response["status_code"] = status

    stop_time = dt.now()
    nodes = query['message']['query_graph']['nodes']
    curie = nodes["ON"].get("ids", nodes["SN"].get("ids"))[0]
    num_results = len((response.get("message") or {}).get("results") or [])
    logging.info(f"{curie} took {stop_time - start_time} seconds and gave {num_results} results")
    # save out responses
    with open(f"{now}_results/{curie}_response.json", "w") as f:
        json.dump(response, f, indent=2)


if __name__ == "__main__":
    os.makedirs(f"{now}_results", exist_ok=True)
    with open("normalized_queries.json", "r") as f:
        queries = json.load(f)
    for query in tqdm(queries):
        single_lookup(query)
