from oss_know.libs.util.log import logger
# crawl
import oss_know.libs.util.zulip_base as zulip_base
import time
import zulip
# from random import randint
from tenacity import *
import requests
import urllib3

from oss_know.libs.util.base import get_opensearch_client
from opensearchpy import helpers


def get_data_from_zulip(email: str, api_key: str, site: str, target: str):
    client = zulip.Client(email=email, api_key=api_key, site=site)
    logger.info("Start crawling data of %s." % (target))
    if target == "members":
        result = client.get_members({"include_custom_profile_fields": True})
    if target == "presences":
        result = client.get_realm_presence()
    if target == "user_groups":
        result = client.get_user_groups()
    if target == "streams":
        result = client.get_streams()
    return result


def get_data_from_opensearch(index, owner, repo, opensearch_conn_info):
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    results = helpers.scan(client=opensearch_client,
                           query={
                                "track_total_hits": True,
                                "query": {
                                    "bool": {
                                        "must": [
                                            {"match": {"search_key.owner.keyword": owner}},
                                            {"match": {"search_key.repo.keyword" : repo}}
                                        ]
                                    }
                                }
                           },
                           index=index,
                           request_timeout=120,
                           preserve_order=True)
    return results


def get_topic_data_from_opensearch(index, owner, repo, stream_id, opensearch_conn_info):
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    results = helpers.scan(client=opensearch_client,
                           query={
                                "track_total_hits": True,
                                "query": {
                                    "bool": {
                                        "must": [
                                            {"match": {"search_key.owner.keyword": owner}},
                                            {"match": {"search_key.repo.keyword" : repo}},
                                            {"match": {"search_key.stream_id": stream_id}}
                                        ]
                                    }
                                }
                           },
                           index=index,
                           request_timeout=120,
                           preserve_order=True)
    return results


def get_latest_message_data_from_opensearch(index, owner, repo, stream_name, topic_name, opensearch_conn_info):
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    # mapping = opensearch_client.indices.get_mapping(index=index)

    results = opensearch_client.search(body={
                                           "query": {
                                                "bool": {
                                                    "must": [
                                                        {"match": {"search_key.owner.keyword": owner}},
                                                        {"match": {"search_key.repo.keyword" : repo}},
                                                        {"match": {"search_key.stream_name.keyword": stream_name}},
                                                        {"match": {"search_key.topic_name.keyword": topic_name}}
                                                    ]
                                                }
                                            },
                                            "sort": [
                                                {
                                                    "raw_data.id": {
                                                        "order": "desc"
                                                    }
                                                }
                                            ]
                                       },
                                       index=index,
                                       size=1,
                                       request_timeout=120)
    return results["hits"]["hits"]
