import time
import datetime
from random import randint
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.log import logger
from oss_know.libs.util.zulip_base import get_data_from_opensearch, get_data_from_zulip
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_ZULIP_USER_GROUP
from oss_know.libs.util.opensearch_api import OpensearchAPI


def crawl_zulip_user_group(owner, repo, email, api_key, site, opensearch_conn_info):
    result = get_data_from_zulip(email=email, api_key=api_key, site=site, target="user_groups")["user_groups"]
    opensearch_data = get_data_from_opensearch(OPENSEARCH_ZULIP_USER_GROUP, owner, repo, opensearch_conn_info)
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    opensearch_api = OpensearchAPI()
    index_exist = opensearch_client.indices.exists(index=OPENSEARCH_ZULIP_USER_GROUP)
    if not index_exist:
        response = opensearch_client.indices.create(index=OPENSEARCH_ZULIP_USER_GROUP)
        logger.info(f"Initial OPENSEARCH_ZULIP_USER_GROUP.")

    group_exist = dict()
    for item in opensearch_data:
        group_exist[item["_source"]["raw_data"]["id"]] = True

    bulk_data = []
    new_user_group = 0
    for item in result:
        if item["id"] in group_exist:
            continue
        bulk_data.append({
            "_index": OPENSEARCH_ZULIP_USER_GROUP,
            "_source":{
                "search_key": {
                    "owner": owner,
                    "repo": repo,
                    "origin": site,
                    "updated_at": round(datetime.datetime.now().timestamp())
                },
                "raw_data": item
            }
        })
        new_user_group += 1

    success, failure = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                        bulk_all_data=bulk_data,
                                                        owner=owner,
                                                        repo=repo)

    logger.info(f"sync_bulk_zulip_data::success:{success},failure:{failure}")
    logger.info(f"count:{new_user_group}::{owner}/{repo}")
    bulk_data.clear()
    return
