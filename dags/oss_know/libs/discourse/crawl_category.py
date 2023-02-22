import copy
import datetime
import time
import shutil
import os
from loguru import logger
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_DISCOURSE_CATEGORY
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.opensearch_api import OpensearchAPI

# crawl
import json
import requests

def delete_old_data(owner, repo, client):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {
                        "search_key.owner.keyword": {
                            "value": owner
                        }
                    }}, {"term": {
                        "search_key.repo.keyword": {
                            "value": repo
                        }
                    }}
                ]
            }
        }
    }
    response = client.delete_by_query(index=OPENSEARCH_DISCOURSE_CATEGORY, body=query)
    logger.info(response)

def crawl_discourse_category(base_url, owner, repo, proxy_config, opensearch_conn_datas):
    # url = f"https://discuss.pytorch.org/categories.json"
    url = base_url + "/categories.json"

    session = requests.Session()
    headers = {"charset": "utf-8", "Content-Type": "application/json"}
    while(True):
        flag = 1
        try:
            r = session.get(url, headers=headers, timeout=10)
        except Exception:
            flag = 0
            pass
        if flag and r.ok:
            break
    js = json.loads(r.text)

    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_datas)

    delete_old_data(owner=owner, repo=repo, client=opensearch_client)

    bulk_data_tp = {"_index": OPENSEARCH_DISCOURSE_CATEGORY,
                    "_source": {
                        "search_key": {
                            "owner": owner,
                            "repo": repo,
                            "origin": url,
                            'updated_at': 0,
                            'if_sync':0
                        },
                        "raw_data": {}
                    }}
    
    now_count = 0
    all_category_list = []
    # 实例化opensearchapi
    opensearch_api = OpensearchAPI()
    for info in js['category_list']['categories']:
        bulk_data = copy.deepcopy(bulk_data_tp)
        bulk_data["_source"]["search_key"]["updated_at"] = int(datetime.datetime.now().timestamp() * 1000)
        bulk_data["_source"]["raw_data"] = info
        now_count = now_count + 1
        all_category_list.append(bulk_data)

    success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                        bulk_all_data=all_category_list,
                                                        owner=owner,
                                                        repo=repo)
    
    logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
    logger.info(f"count:{now_count}::{owner}/{repo}")
    all_category_list.clear()
    return