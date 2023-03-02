import copy
import datetime
import time
import shutil
import os
from oss_know.libs.util.log import logger
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_DISCOURSE_USER_LIST, OPENSEARCH_DISCOURSE_USER_INFO
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.opensearch_api import OpensearchAPI

# crawl
import json
import requests
import time
from tqdm import tqdm

from random import randint

from oss_know.libs.util.discourse import get_api
from oss_know.libs.util.discourse import get_data_from_opensearch

def crawl_user_info(base_url, owner, repo, opensearch_conn_datas):
    # 从opensearch中取回 user list
    opensearch_datas = get_data_from_opensearch(OPENSEARCH_DISCOURSE_USER_LIST, owner, repo, opensearch_conn_datas)
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_datas)
    opensearch_api = OpensearchAPI()

    user_list = []
    for user_data in opensearch_datas[0]:
        user = user_data['_source']['raw_data']
        user_list.append(user)

    bulk_data_tp = {"_index": OPENSEARCH_DISCOURSE_USER_INFO,
                "_source": {
                    "search_key": {
                        "owner": owner,
                        "repo": repo,
                        "origin": base_url,
                        'updated_at': round(datetime.datetime.now().timestamp()),
                        'if_sync':0
                    },
                    "raw_data": {
                        "user_summary":{},
                        "user_badges": {}
                    }
                }}
    
    user_list_len = len(user_list)
    logger.info(f"Current user count = {user_list_len}")
    all_user_list = []
    now_count = 0
    
    for idx, user in enumerate(tqdm(user_list)):
        session = requests.Session()
        user_name = user['username']
        url = f'{base_url}/u/{user_name}/'
        summary_url = f'{base_url}/u/{user_name}/summary.json'
        badges_url = f'{base_url}/u/{user_name}/badges.json'

        user_summary = get_api(summary_url, session)
        user_badges = get_api(badges_url, session)

        bulk_data = copy.deepcopy(bulk_data_tp)
        bulk_data["_source"]["search_key"]["origin"] = url
        bulk_data["_source"]["raw_data"]['user_summary'] = user_summary
        bulk_data["_source"]["raw_data"]['user_badges'] = user_badges

        all_user_list.append(bulk_data)

        now_count = now_count + 1
        if now_count % 500 == 0:
            success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                                bulk_all_data=all_user_list,
                                                                owner=owner,
                                                                repo=repo)
            logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
            logger.info(f"count:{now_count}/{user_list_len}::{owner}/{repo}")
            all_user_list.clear()
    
    success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                    bulk_all_data=all_user_list,
                                                    owner=owner,
                                                    repo=repo)
    
    logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
    logger.info(f"count:{now_count}::{owner}/{repo}")
    all_user_list.clear()

    return