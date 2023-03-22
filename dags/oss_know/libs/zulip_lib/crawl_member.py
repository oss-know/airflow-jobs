import time
import datetime
from random import randint
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.log import logger
from oss_know.libs.util.zulip_base import get_data_from_opensearch, get_data_from_zulip
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_ZULIP_MEMBER
from oss_know.libs.util.opensearch_api import OpensearchAPI


def crawl_zulip_member(owner, repo, email, api_key, site, opensearch_conn_info):
    result = get_data_from_zulip(email=email, api_key=api_key, site=site, target="members")["members"]
    opensearch_data = get_data_from_opensearch(OPENSEARCH_ZULIP_MEMBER, owner, repo, opensearch_conn_info)

    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    opensearch_api = OpensearchAPI()

    member_exist = dict()
    for item in opensearch_data:
        member_exist[item["_source"]["raw_data"]["user_id"]] = True
    logger.info(f"{len(member_exist)} data have already existed.")

    bulk_data = []
    new_member = 0
    for item in result:
        if item["user_id"] in member_exist:
            continue
        if "profile_data" in item:
            if "3873" in item["profile_data"] and "3877" in item["profile_data"]:
                item["profile_data"] = {"github_name": item["profile_data"]["3873"], "pronoun": item["profile_data"]["3877"]}
            elif "3873" in item["profile_data"]:
                item["profile_data"] = {"github_name": item["profile_data"]["3873"], "pronoun": None}
            elif "3877" in item["profile_data"]:
                item["profile_data"] = {"github_name": None, "pronoun": item["profile_data"]["3877"]}
            else:
                item["profile_data"] = {"github_name": None, "pronoun": None}
        else:
            item["profile_data"] = {"github_name": None, "pronoun": None}
        bulk_data.append({
            "_index": OPENSEARCH_ZULIP_MEMBER,
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
        new_member += 1

    success, failure = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                        bulk_all_data=bulk_data,
                                                        owner=owner,
                                                        repo=repo)

    logger.info(f"sync_bulk_zulip_data::success:{success},failure:{failure}")
    logger.info(f"count:{new_member}::{owner}/{repo}")
    bulk_data.clear()
    return
