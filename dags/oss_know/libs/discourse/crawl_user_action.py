import copy
import datetime
import time
import shutil
import os
from loguru import logger
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_DISCOURSE_USER_LIST, OPENSEARCH_DISCOURSE_USER_ACTION
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.opensearch_api import OpensearchAPI

# crawl
import json
import requests
import time

from opensearchpy import helpers
from tqdm import tqdm
from random import randint
'''
需要传入三个参数
username    ：
offset      ：分页查询
fliter      ：action类型：详见https://github.com/discourse/discourse/blob/main/app/models/user_action.rb
    LIKE = 1
    WAS_LIKED = 2
    NEW_TOPIC = 4
    REPLY = 5
    RESPONSE = 6
    MENTION = 7
    QUOTE = 9
    EDIT = 11
    NEW_PRIVATE_MESSAGE = 12
    GOT_PRIVATE_MESSAGE = 13
    SOLVED = 15
    ASSIGNED = 16
'''

ALL_TYPE="1,2,4,5,6,7,9,11,12,13,15,16"

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


def get_user_action(url, user_name, session, fliter=ALL_TYPE):
    res = []
    flag = 1
    offset = 0
    while(flag):
        cur_url = f'{url}/user_actions.json?offset={offset}&fliter={fliter}&username={user_name}'
        js = get_api(cur_url, session)

        if(not js or len(js['user_actions'])==0):
            break
        offset += len(js['user_actions'])
        res.extend(js['user_actions'])
    return res


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


def crawl_user_action(base_url, owner, repo, opensearch_conn_datas):
    # 从opensearch中取回 user list
    opensearch_datas = get_data_from_opensearch(OPENSEARCH_DISCOURSE_USER_LIST, owner, repo, opensearch_conn_datas)
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_datas)
    opensearch_api = OpensearchAPI()

    user_list = []
    for user_data in opensearch_datas[0]:
        user = user_data['_source']['raw_data']
        user_list.append(user)

    bulk_data_tp = {"_index": OPENSEARCH_DISCOURSE_USER_ACTION,
                "_source": {
                    "search_key": {
                        "owner": owner,
                        "repo": repo,
                        "origin": base_url,
                        'updated_at': round(datetime.datetime.now().timestamp()),
                        'if_sync':0
                    },
                    "raw_data": {
                        "user_action": [],
                    }
                }}
    
    user_list_len = len(user_list)
    logger.info(f"Current user count = {user_list_len}")
    all_user_list = []
    now_count = 0

    for idx, user in enumerate(tqdm(user_list)):
        session = requests.Session()
        user_name = user['username']

        user_action = get_user_action(base_url, user_name, session)

        bulk_data = copy.deepcopy(bulk_data_tp)
        bulk_data["_source"]["search_key"]["origin"] = base_url
        bulk_data["_source"]["raw_data"]['user_action'] = user_action

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