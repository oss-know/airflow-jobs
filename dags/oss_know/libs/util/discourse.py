from oss_know.libs.util.log import logger
# crawl
import json
import requests
import time

from random import randint

from oss_know.libs.util.base import get_opensearch_client
from opensearchpy import helpers

def get_api(url, session):
    headers = {"charset": "utf-8", "Content-Type": "application/json"}
    max_try = 20
    while(True):
        flag = 1
        try:
            r = session.get(url, headers=headers, timeout=10)
        except Exception:
            flag = 0
        if flag and r.ok:
            break
        time.sleep(randint(5,60))
        max_try -= 1
        if max_try == 0:
            logger.info(f"ERROR of {url}")
            return {}
    js = json.loads(r.text)
    return js


def get_data_from_opensearch(index, owner, repo, opensearch_conn_datas):
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_datas)
    results = helpers.scan(client=opensearch_client,
                           query={
                               "query": {
                                    "bool": {
                                        "must": [
                                            {"match": {"search_key.owner.keyword": owner}},
                                            {"match": {"search_key.repo.keyword" : repo}}
                                        ]
                                    }
                                },
                               "sort": [
                                   {
                                       "search_key.updated_at": {
                                           "order": "asc"
                                       }
                                   }
                               ]
                           },
                           index=index,
                           size=5000,
                           scroll="40m",
                           request_timeout=120,
                           preserve_order=True)
    return results, opensearch_client

