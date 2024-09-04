import argparse
import json
import logging

import httpx
from datetime import datetime as dt
from tqdm import tqdm
import traceback

aragorn_urls = {
    "prod": "https://aragorn.transltr.io/aragorn/query",
    "test": "https://aragorn.test.transltr.io/aragorn/query",
    "ci": "https://aragorn.ci.transltr.io/aragorn/query",
    "dev": "https://aragorn.renci.org/aragorn/query",
}

def create_mvp1_query(disease_id):
    return {
        "message": {
            "query_graph": {
                "nodes": {
                    "on": {
                        "ids": [disease_id],
                        "categories": ["biolink:Disease"],
                    },
                    "sn": {
                        "categories": ["biolink:ChemicalEntity"],
                    },
                },
                "edges": {
                    "t_edge": {
                        "object": "on",
                        "subject": "sn",
                        "predicates": ["biolink:treats"],
                        "knowledge_type": "inferred",
                    },
                },
            },
        }
    }


def run_query(url, query, curie, results):
    try:
        with httpx.Client(timeout=query["parameters"]["timeout_seconds"] + 300) as client:
            dr = client.post(url,json=query)
            results[curie]["status"] = dr.status_code
            if dr.status_code != 200:
                logging.error(f"{curie} failed with status code {dr.status_code}")
                return
            try:
                nresults = len(dr.json()["message"]["results"])
                results[curie]["num_results"] = nresults
                if nresults == 0:
                    logging.warning(f"{curie} had no results")
                    return
                logging.info(f"{curie} had {nresults} results")
            except Exception:
                logging.error(f"Something went wrong with {curie}")
    except httpx.ReadTimeout as e:
        results[curie]["status"] = 408
        logging.error(f"{curie} timed out")
    except Exception as e:
        results[curie]["status"] = 418
        logging.error(f"Unhandled error for {curie}: {traceback.format_exc()}")


def main(env, curies, results_path=None):
    url = aragorn_urls[env]
    results = {}
    output_filename = f"{env}/{dt.now().strftime('%Y_%m_%d_%H_%M_%S')}_results.json"
    if results_path is not None:
        # grab existing results and continue
        with open(results_path, "r") as f:
            results = json.load(f)
    for curie in tqdm(curies):
        if curie not in results:
            start = dt.now()
            query = create_mvp1_query(curie)
            results[curie] = {}
            run_query(url, query, curie, results)
            end = dt.now()
            total_time = end - start
            results[curie]["time"] = total_time.total_seconds()
            logging.info(f"Took {total_time}")
            logging.info("===================")
            with open(output_filename, "w") as f:
                json.dump(results, f)
        else:
            logging.info(f"Already got response for {curie}")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ARAGORN Caching Script")
    parser.add_argument(
        "env",
        type=str,
        help="Environment to fill the cache on.",
        choices=["dev", "ci", "test", "prod"],
    )

    parser.add_argument(
        "--results_path",
        type=str,
        help="Use an existing results file to resume cache filling",
    )

    args = parser.parse_args()

    logging.basicConfig(filename=f"{args.env}/{dt.now().strftime('%Y_%m_%d_%H_%M_%S')}.log", level=logging.INFO, format="[%(asctime)s: %(levelname)s/%(name)s]: %(message)s")
    with open("mondo_curies.json", "r") as f:
        curies = json.load(f)
    with open("hpo_curies.json", "r") as f:
        curies = curies + json.load(f)

    main(args.env, curies, args.results_path)
