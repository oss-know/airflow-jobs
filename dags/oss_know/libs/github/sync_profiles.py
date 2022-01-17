import datetime
import itertools
import requests
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_PROFILE, OPENSEARCH_INDEX_CHECK_SYNC_DATA
from oss_know.libs.util.log import logger
from oss_know.libs.util.opensearch_api import OpensearchAPI


def sync_github_profiles(github_tokens, opensearch_conn_info):
    """Update existing GitHub profiles by id."""

    github_tokens_iter = itertools.cycle(github_tokens)

    opensearch_client = get_opensearch_client(opensearch_conn_info)

    # 取得上次更新github profile 的位置，用于判断循环起点
    has_profile_check = opensearch_client.search(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA,
                                                 body={
                                                     "size": 1,
                                                     "track_total_hits": True,
                                                     "query": {
                                                         "bool": {
                                                             "must": [
                                                                 {
                                                                     "term": {
                                                                         "search_key.type.keyword": {
                                                                             "value": "github_profiles"
                                                                         }
                                                                     }
                                                                 }
                                                             ]
                                                         }
                                                     },
                                                     "sort": [
                                                         {
                                                             "search_key.update_timestamp": {
                                                                 "order": "desc"
                                                             }
                                                         }
                                                     ]
                                                 }
                                                 )

    if has_profile_check["hits"]["hits"]:
        existing_github_id = has_profile_check["hits"]["hits"][0]["_source"]["github"]["id"]
        print("上次存储的check node的id为：",existing_github_id)
    else:
        existing_github_id = -1

    # 将折叠（collapse）查询opensearch（去重排序）的结果作为查询更新的数据源
    # collapse和scan、scroll无法同时使用，为保证效率， 将每次获取的数据量设定为1000
    existing_github_profiles = opensearch_client.search(
        index=OPENSEARCH_INDEX_GITHUB_PROFILE,
        body={
            "query": {
                "range": {
                    "id": {
                        "gte": existing_github_id
                    }
                }
            },
            "collapse": {
                "field": "id"
            },
            "sort": [
                {
                    "id": {
                        "order": "asc"
                    }
                },
                {
                    "updated_at": {
                        "order": "desc"
                    }
                }
            ],
            "size": 1000
        }
    )

    if not existing_github_profiles:
        logger.info("There's no existing github profiles")
        return "END::There's no existing github profiles"

    github_api = GithubAPI()
    session = requests.Session()
    opensearch_api = OpensearchAPI()

    # end_time = (datetime.datetime.now() + datetime.timedelta(hours=3)).timestamp()
    end_time = (datetime.datetime.now() + datetime.timedelta(microseconds=4000)).timestamp()
    current_profile_id = None
    for existing_github_profile in existing_github_profiles['hits']['hits']:
        current_profile_id = existing_github_profile["_source"]["id"]
        current_profile_login = existing_github_profile["_source"]["login"]


        existing_profile_updated_at = existing_github_profile["_source"]["updated_at"]

        latest_github_profile = github_api.get_github_profiles(http_session=session,
                                                               github_tokens_iter=github_tokens_iter,
                                                               id_info=current_profile_id)
        latest_profile_updated_at = latest_github_profile["updated_at"]

        if existing_profile_updated_at != latest_profile_updated_at:
            opensearch_client.index(index=OPENSEARCH_INDEX_GITHUB_PROFILE,
                                    body=latest_github_profile,
                                    refresh=True)
            logger.info(f"Success put updated {current_profile_login}'s github profiles into opensearch.")
        else:
            logger.info(f"{current_profile_login}'s github profiles of opensearch is latest.")
        # 判断当前时间是否为查询有效时间（当日零点到凌晨三点之间：一小时只查询比较不存储约处理360个profile,为保证高效可用，每次查询时间由2小时延长至3小时）
        if datetime.datetime.now().timestamp() > end_time:
            logger.info('The connection has timed out.')
            break

    check_node_profile = opensearch_client.search(index=OPENSEARCH_INDEX_GITHUB_PROFILE,
                                                 body={
                                                     "query": {
                                                         "range": {
                                                             "id": {
                                                                 "gt": current_profile_id
                                                             }
                                                         }
                                                     },
                                                     "sort": [
                                                         {
                                                             "id": {
                                                                 "order": "asc"
                                                             }
                                                         }],
                                                     "size": 1
                                                 }
                                                 )
    if  check_node_profile["hits"]["hits"]:
        check_node_id = check_node_profile["hits"]["hits"][0]["_source"]["id"]
        check_node_login = check_node_profile["hits"]["hits"][0]["_source"]["login"]
    else:
        check_node_id = -1
        check_node_login = None
    opensearch_api.set_sync_github_profiles_check(opensearch_client=opensearch_client, login=check_node_login,
                                                  id=check_node_id)
    return "END::sync_github_profiles"


# TODO: 传入用户的profile信息，获取用户的location、company、email，与晨琪对接
def github_profile_data_source(now_github_profile):
    # todo: the print statements just for testing, w can ignore them
    now_github_profile_user_company = now_github_profile["company"]
    if now_github_profile_user_company is not None:
        print("now_github_profile_user_company")
        print(now_github_profile_user_company)

    now_github_profile_user_location = now_github_profile["location"]
    if now_github_profile_user_location is not None:
        print("now_github_profile_user_location")
        print(now_github_profile_user_location)

    now_github_profile_user_email = now_github_profile["email"]
    if now_github_profile_user_email is not None:
        print("now_github_profile_user_email")
        print(now_github_profile_user_email)
    return "与晨琪对接哈"
