import time
import datetime
from random import randint
import zulip
from tenacity import *
import requests
import urllib3
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.log import logger
from oss_know.libs.util.zulip_base import get_data_from_opensearch, get_latest_message_data_from_opensearch
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_ZULIP_TOPIC, OPENSEARCH_ZULIP_MESSAGE
from oss_know.libs.util.opensearch_api import OpensearchAPI
from opensearchpy import helpers

@retry(stop=stop_after_attempt(20),
       wait=wait_random(20, 40),
       retry=(retry_if_exception_type(urllib3.exceptions.HTTPError) |
              retry_if_exception_type(urllib3.exceptions.MaxRetryError) |
              retry_if_exception_type(urllib3.exceptions.ReadTimeoutError) |
              retry_if_exception_type(requests.exceptions.ProxyError) |
              retry_if_exception_type(requests.exceptions.SSLError) |
              retry_if_exception_type(requests.exceptions.ReadTimeout) |
              retry_if_result(lambda x:x["result"] == "error")),
        reraise=True)
def message_request(client, request):
    result = client.get_messages(request)
    return result


def get_messages(client, stream_name, topic_name, latest_anchor):
    # logger.info(f"Start crawling data of {stream_name}::{topic_name}::messages.")

    # total_message_num = 0
    results = []
    found_newest = False
    anchor = latest_anchor
    while not found_newest:
        request = {
            "anchor": anchor,
            "include_anchor": False,
            "num_before": 0,
            "num_after": 100,
            "narrow": [
                {"operator": "stream", "operand": stream_name},
                {"operator": "topic", "operand": topic_name},
            ],
        }
        result = message_request(client, request)
        if result["messages"]:
            anchor = result["messages"][-1]["id"]
        found_newest = result["found_newest"]
        results.extend(result["messages"])
        time.sleep(1)
    logger.info(f"total_count of {stream_name}::{topic_name}:{len(results)}")
    return results


def crawl_zulip_message(owner, repo, email, api_key, site, opensearch_conn_info):
    client = zulip.Client(email=email, api_key=api_key, site=site)

    topic_data = get_data_from_opensearch(OPENSEARCH_ZULIP_TOPIC, owner, repo, opensearch_conn_info)
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    index_exist = opensearch_client.indices.exists(index=OPENSEARCH_ZULIP_MESSAGE)
    if not index_exist:
        response = opensearch_client.indices.create(index=OPENSEARCH_ZULIP_MESSAGE)
        logger.info(f"Initial OPENSEARCH_ZULIP_MESSAGE.")
    opensearch_api = OpensearchAPI()

    new_message = 0
    for topic in topic_data:
        stream_name = topic["_source"]["raw_data"]["stream_name"]
        topic_name = topic["_source"]["raw_data"]["topic_name"]
        
        latest_anchor = "oldest"
        if index_exist:
            opensearch_data = get_latest_message_data_from_opensearch(OPENSEARCH_ZULIP_MESSAGE, owner, repo, stream_name, topic_name, opensearch_conn_info)

            for item in opensearch_data:
                latest_anchor = item["_source"]["raw_data"]["id"]
                # logger.info(f"anchor of {topic_name} is {latest_anchor}")
                break
            

        result = get_messages(client, stream_name, topic_name, latest_anchor)

        bulk_data = []

        for item in result:
            item["stream_name"] = item.pop("display_recipient")
            item["topic_name"] = item.pop("subject")
            bulk_data.append({
                "_index": OPENSEARCH_ZULIP_MESSAGE,
                "_source":{
                    "search_key": {
                        "owner": owner,
                        "repo": repo,
                        "origin": site,
                        "updated_at": round(datetime.datetime.now().timestamp()),
                        "stream_name": stream_name,
                        "topic_name": topic_name
                    },
                    "raw_data": item
                }
            })
            new_message += 1

        success, failure = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                            bulk_all_data=bulk_data,
                                                            owner=owner,
                                                            repo=repo)

        if success != 0:
            logger.info(f"sync_bulk_zulip_data::{stream_name}::{topic_name}::success:{success},failure:{failure}")
        bulk_data.clear()
        # if new_message != 0 and new_message > 10:
        #     break
    logger.info(f"count:{new_message}::{owner}/{repo}")
    return
