import copy
import datetime
import time
import shutil
import os
from loguru import logger
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_DISCOURSE_TOPIC_CONTENT, OPENSEARCH_DISCOURSE_USER_LIST
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.opensearch_api import OpensearchAPI

from opensearchpy import helpers

def get_data_from_opensearch(index, owner, repo, opensearch_conn_datas):
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_datas)
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
    return results, opensearch_client


def parse_user_from_topic(base_url, owner, repo, opensearch_conn_datas):
    # 从opensearch中取回 topic content
    opensearch_datas = get_data_from_opensearch(OPENSEARCH_DISCOURSE_TOPIC_CONTENT, owner, repo, opensearch_conn_datas)
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_datas)
    opensearch_api = OpensearchAPI()

    bulk_data_tp = {"_index": OPENSEARCH_DISCOURSE_USER_LIST,
                    "_source": {
                        "search_key": {
                            "owner": owner,
                            "repo": repo,
                            "origin": base_url,
                            'updated_at': round(datetime.datetime.now().timestamp()),
                            'if_sync':0
                        },
                        "raw_data": {}
                    }}
    
    vis = {}
    all_user_list = []
    now_count = 0
    for topic_data in opensearch_datas[0]:
        for user_data in topic_data['_source']['raw_data']['details']['participants']:
            if user_data['id'] in vis:
                continue
            vis[user_data['id']] = 1

            bulk_data = copy.deepcopy(bulk_data_tp)
            bulk_data["_source"]["raw_data"] = user_data
            all_user_list.append(bulk_data)
            
            now_count = now_count + 1
            if now_count % 500 == 0:
                success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                                    bulk_all_data=all_user_list,
                                                                    owner=owner,
                                                                    repo=repo)
                logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
                logger.info(f"count:{now_count}::{owner}/{repo}")
                all_user_list.clear()

    success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                        bulk_all_data=all_user_list,
                                                        owner=owner,
                                                        repo=repo)
    logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
    logger.info(f"count:{now_count}::{owner}/{repo}")
    all_user_list.clear()
