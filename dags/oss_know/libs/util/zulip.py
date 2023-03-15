from oss_know.libs.util.log import logger
# crawl
import zulip
import time

from random import randint

from oss_know.libs.util.base import get_opensearch_client
from opensearchpy import helpers

def get_api(email: str, api_key: str, site: str):
    client = zulip.Client(email=email, api_key=api_key, site=site)
    return client


def get_data_from_opensearch(index, owner, repo, opensearch_conn_info):
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    results = helpers.scan(client=opensearch_client,
                           query={
                               "query": {
                                    "bool": {
                                        "must": [
                                            {"match": {"search_key.owner": owner}},
                                            {"match": {"search_key.repo" : repo}}
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
    return results
