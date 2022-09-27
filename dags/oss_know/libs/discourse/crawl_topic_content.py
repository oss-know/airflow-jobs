import copy
import datetime
import time
import shutil
import os
from loguru import logger
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_DISCOURSE_TOPIC_CONTENT, OPENSEARCH_DISCOURSE_TOPIC_LIST
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.opensearch_api import OpensearchAPI

# crawl
import json
import requests
import time

from opensearchpy import helpers

# 协程
import httpx
import asyncio
# import tqdm
import tqdm.asyncio
import random

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


async def make_request(client, url, sem):
    async with sem:
        max_try = 100
        while(True):
            url = url
            flag = 1
            headers = {"charset": "utf-8", "Content-Type": "application/json"}
            try:
                resp = await client.get(url, headers=headers, timeout=2)
            except:
                flag = 0
            if not flag or resp.status_code!=200:
                
                if max_try > 0:
                    max_try -= 1
                    time.sleep(random.randint(2,3))
                    continue
                else:
                    print(f"ERRRRRRRRRRRRRRRRRRRRRRRRRRRRRRROR!!! {url}")
                    return None

            js = json.loads(resp.text)
            result = {}
            result['url'] = url
            result['content'] = js
            return result


async def multi_request(tasks):
    sem = asyncio.Semaphore(3)
    res = []
    async with httpx.AsyncClient() as client:
        httpx_tasks = []
        for url in tasks:
            task = asyncio.create_task(make_request(client, url, sem))
            httpx_tasks.append(task)
        try:
            # res = [await f
            #             for f in asyncio.as_completed(httpx_tasks)]
            res = [await f
                        for f in tqdm.tqdm(asyncio.as_completed(httpx_tasks), total=len(httpx_tasks))]
        except asyncio.TimeoutError:
            print('Timeout Error!')
    return res


def single_request(tasks):
    session = requests.Session()
    res = []
    for url in tasks:
        js = get_api(url, session)
        result = {}
        result['url'] = url
        result['content'] = js
        res.append(result)
    return res


def solve(tasks, bulk_data_tp):
    
    # 好像速度没啥提升
    # res_tasks = asyncio.run(multi_request(tasks)) 

    res_tasks = single_request(tasks)
    
    all_topic_content = []
    now_count = 0
    for res in res_tasks:
        url = res['url']
        topic_content = res['content']
        bulk_data = copy.deepcopy(bulk_data_tp)
        bulk_data["_source"]["search_key"]["origin"] = url
        bulk_data["_source"]["raw_data"] = topic_content
        all_topic_content.append(bulk_data)
        now_count = now_count + 1

    return all_topic_content, now_count


def crawl_topic_content(base_url, owner, repo, opensearch_conn_datas):
    # url format = https://discuss.pytorch.org/t/gradients-w-r-t-loss-component/161814.json

    # 从opensearch中取回category list
    opensearch_datas = get_data_from_opensearch(OPENSEARCH_DISCOURSE_TOPIC_LIST, opensearch_conn_datas)
    topic_list = []
    q = 0   # currently only 2015 results.  
    for page_data in opensearch_datas[0]:
        q += 1
        for topic_data in page_data["_source"]["raw_data"]["topic_list"]["topics"]:
            topic = base_url + '/t/' + topic_data["slug"] + '/' + str(topic_data["id"]) + ".json"
            topic_list.append(topic)

    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)
    opensearch_api = OpensearchAPI()

    bulk_data_tp = {"_index": OPENSEARCH_DISCOURSE_TOPIC_CONTENT,
                    "_source": {
                        "search_key": {
                            "owner": owner,
                            "repo": repo,
                            "origin": base_url,
                            'updated_at': 0,
                            'if_sync':0
                        },
                        "raw_data": {}
                    }}

    topic_list_len = len(topic_list)    # 59564
    now_count = 0
    cur_tasks = []
    for idx, topic in enumerate(topic_list):
        # Divide to small block,
        # every block contains 500 topic.
        cur_tasks.append(topic)
        if (idx + 1) % 500 == 0:
            logger.info(f"start solve {idx-499} ~ {idx+1}")
            all_topic_content, cur_count = solve(cur_tasks, bulk_data_tp)
            cur_tasks.clear()
            now_count += cur_count
            success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                                bulk_all_data=all_topic_content,
                                                                owner=owner,
                                                                repo=repo)

            logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
            logger.info(f"count:{now_count}/{topic_list_len}::{owner}/{repo}")

            # tmp
            break
    all_topic_content, cur_count = solve(cur_tasks, bulk_data_tp)
    cur_tasks.clear()
    now_count += cur_count
    success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                    bulk_all_data=all_topic_content,
                                                    owner=owner,
                                                    repo=repo)
    
    logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
    logger.info(f"count:{now_count} / {topic_list_len}::{owner}/{repo}")
    all_topic_content.clear()

    return 
