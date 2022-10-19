import copy
import datetime
import time
import shutil
import os
from loguru import logger
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_DISCOURSE_CATEGORY, OPENSEARCH_DISCOURSE_TOPIC_LIST
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.opensearch_api import OpensearchAPI

# crawl
import json
import requests
import time

from opensearchpy import helpers

def get_api(url, session):
    headers = {"charset": "utf-8", "Content-Type": "application/json"}
    while(True):
        flag = 1
        try:
            r = session.get(url, headers=headers, timeout=10)
        except Exception:
            time.sleep(2)
            flag = 0
            pass
        if flag and r.ok:
            break
    js = json.loads(r.text)
    return js

def get_data_from_opensearch(index, opensearch_conn_datas):
    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)
    results = helpers.scan(client=opensearch_client,
                           query={
                               "query": {"match_all": {}},
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
                           request_timeout=100,
                           preserve_order=True)
    return results, opensearch_client


def crawl_topic_list(base_url, owner, repo, opensearch_conn_datas):

    # 从opensearch中取回category list
    opensearch_datas = get_data_from_opensearch(OPENSEARCH_DISCOURSE_CATEGORY, opensearch_conn_datas)
    category_list = []
    for category_data in opensearch_datas[0]:
        category = '/c/' + category_data["_source"]["raw_data"]["slug"] + '/' + str(category_data["_source"]["raw_data"]["id"])
        category_list.append((category, category_data["_source"]["raw_data"]["slug"], category_data["_source"]["raw_data"]["id"]))
        # logger.info(f"########{category}########")

    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)
    opensearch_api = OpensearchAPI()

    bulk_data_tp = {"_index": OPENSEARCH_DISCOURSE_TOPIC_LIST,
                "_source": {
                    "search_key": {
                        "owner": owner,
                        "repo": repo,
                        "category_id": 0,
                        "category_name": "",
                        'updated_at': 0,
                        'if_sync':0
                    },
                   "raw_data": {}
                }}

    all_topic_list = []
    category_cnt = len(category_list)
    for idx, category in enumerate(category_list):
        session = requests.Session()
        now_count = 0
        cur_page = 0
        get_list_cnt = 0
        flag = 1
        category , category_name, category_id = category

        while(flag):
            logger.info(f"Working about {category} [{idx}/{category_cnt}] at page {cur_page}, cur get {get_list_cnt} discussion!")
            
            url = f"{base_url}/{category}.json?page={cur_page}"

            disscuss_list = get_api(url, session)          

            for topics in disscuss_list['topic_list']['topics']:

                bulk_data = copy.deepcopy(bulk_data_tp)
                bulk_data["_source"]["search_key"]["category_id"] = category_id
                bulk_data["_source"]["search_key"]["category_name"] = category_name
                bulk_data["_source"]["raw_data"] = topics

                all_topic_list.append(bulk_data)
                now_count = now_count + 1

                if now_count % 500 == 0:
                    success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                                        bulk_all_data=all_topic_list,
                                                                        owner=owner,
                                                                        repo=repo)
                    logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
                    logger.info(f"count:{now_count}::{owner}/{repo}")
                    all_topic_list.clear()

            get_list_cnt += len(disscuss_list['topic_list']['topics'])
            if "more_topics_url" not in disscuss_list['topic_list']:
                flag = 0
            else:
                cur_page += 1

        success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                        bulk_all_data=all_topic_list,
                                                        owner=owner,
                                                        repo=repo)

        logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
        logger.info(f"count:{now_count}::{owner}/{repo}")
        all_topic_list.clear()
    return 
