import time
import datetime
from random import randint
import zulip
from tenacity import *
import requests
import urllib3
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.log import logger
from oss_know.libs.util.zulip_base import get_data_from_opensearch, get_topic_data_from_opensearch
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_ZULIP_STREAM, OPENSEARCH_ZULIP_TOPIC
from oss_know.libs.util.opensearch_api import OpensearchAPI
from opensearchpy import helpers

@retry(stop=stop_after_attempt(20),
       wait=wait_random(10, 30),
       retry=(retry_if_exception_type(urllib3.exceptions.HTTPError) |
              retry_if_exception_type(urllib3.exceptions.MaxRetryError) |
              retry_if_exception_type(urllib3.exceptions.ReadTimeoutError) |
              retry_if_exception_type(requests.exceptions.ProxyError) |
              retry_if_exception_type(requests.exceptions.SSLError) |
              retry_if_exception_type(requests.exceptions.ReadTimeout) |
              retry_if_result(lambda x:x["result"] == "error")),
        reraise=True)
def get_topics(client, stream_id, stream_name):
    # logger.info(f"Start crawling data of topics::{stream_name}.")

    result = client.get_stream_topics(stream_id)
    for topic in result["topics"]:
        topic["stream_name"] = stream_name
        topic["topic_name"] = topic.pop("name")
    # logger.info(f"total_count:{len(result)}")

    return result


def crawl_zulip_topic(owner, repo, email, api_key, site, opensearch_conn_info):
    client = zulip.Client(email=email, api_key=api_key, site=site)

    stream_data = get_data_from_opensearch(OPENSEARCH_ZULIP_STREAM, owner, repo, opensearch_conn_info)
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    index_exist = opensearch_client.indices.exists(index=OPENSEARCH_ZULIP_TOPIC)
    if not index_exist:
        response = opensearch_client.indices.create(index=OPENSEARCH_ZULIP_TOPIC)
        logger.info(f"Initial OPENSEARCH_ZULIP_MESSAGE.")
    opensearch_api = OpensearchAPI()

    new_topic = 0
    for stream in stream_data:
        stream_id = stream["_source"]["raw_data"]["stream_id"]
        stream_name = stream["_source"]["raw_data"]["name"]

        result = get_topics(client, stream_id, stream_name)["topics"]

        opensearch_data = get_topic_data_from_opensearch(OPENSEARCH_ZULIP_TOPIC, owner, repo, stream_id, opensearch_conn_info)
        topic_exist = dict()
        for item in opensearch_data:
            topic_exist[item["_source"]["raw_data"]["topic_name"]] = True

        bulk_data = []

        for item in result:
            if item["topic_name"] in topic_exist or item["topic_name"] == "(deleted)":
                continue
            bulk_data.append({
                "_index": OPENSEARCH_ZULIP_TOPIC,
                "_source":{
                    "search_key": {
                        "owner": owner,
                        "repo": repo,
                        "origin": site,
                        "updated_at": round(datetime.datetime.now().timestamp()),
                        "stream_id": stream_id
                    },
                    "raw_data": item
                }
            })
            new_topic += 1

        success, failure = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                            bulk_all_data=bulk_data,
                                                            owner=owner,
                                                            repo=repo)

        logger.info(f"sync_bulk_zulip_data::stream:{stream_name}::success:{success},failure:{failure}")
        bulk_data.clear()
    logger.info(f"count:{new_topic}::{owner}/{repo}")
    return
