import time
import datetime
import itertools
import requests
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_PROFILE, OPENSEARCH_INDEX_CHECK_SYNC_DATA
from oss_know.libs.util.log import logger
from oss_know.libs.util.opensearch_api import OpensearchAPI


class SyncGithubProfilesException(Exception):
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status


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

    if not has_profile_check["hits"]["hits"]:
        existing_github_id = has_profile_check["hits"]["hits"][0]["_source"]["github"]["id"]
    else:
        existing_github_id = '0'

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
        logger.error("There's no existing github profiles")
        return "There's no existing github profiles"

    github_api = GithubAPI()
    session = requests.Session()
    opensearch_api = OpensearchAPI()

    end_time = (datetime.datetime.now() + datetime.timedelta(2)).timestamp()

    for existing_github_profile in existing_github_profiles['hits']['hits']:
        next_profile_id = existing_github_profile["_source"]["id"]
        next_profile_login = existing_github_profile["_source"]["login"]
        # 判断当前时间是否为查询有效时间（当日零点到凌晨两点之间）
        if datetime.datetime.now().timestamp() > end_time:
            opensearch_api.set_sync_github_profiles_check(opensearch_client=opensearch_client, login=next_profile_login,
                                                          id=next_profile_id)
            logger.debug(f'Invalid runtime to update existing GitHub profiles by ids.')
            break

        existing_profile_updated_at = existing_github_profile["_source"]["updated_at"]

        now_github_profile = github_api.get_github_profiles(http_session=session,
                                                            github_tokens_iter=github_tokens_iter,
                                                            id_info=next_profile_id)
        now_profile_updated_at = now_github_profile["updated_at"]

        if existing_profile_updated_at != now_profile_updated_at:
            opensearch_client.index(index=OPENSEARCH_INDEX_GITHUB_PROFILE,
                                    body=now_github_profile,
                                    refresh=True)
            logger.info(f"Success put updated {next_profile_login}'s github profiles into opensearch.")
        else:
            logger.info(f"{next_profile_login}'s github profiles of opensearch is latest.")
    the_first_profile= opensearch_client.search(index=OPENSEARCH_INDEX_GITHUB_PROFILE,
                                                 body={
                                                     "query": {
                                                         "match_all": {}
                                                     },
                                                     "collapse": {
                                                         "field": "id"
                                                     },
                                                     "size": 1,
                                                     "sort": [
                                                         {
                                                             "id": {
                                                                 "order": "asc"
                                                             }
                                                         }
                                                     ]
                                                 }
                                                 )
    the_first_github_id = the_first_profile["hits"]["hits"][0]["_source"]["id"]
    the_first_github_login = the_first_profile["hits"]["hits"][0]["_source"]["login"]
    opensearch_api.set_sync_github_profiles_check(opensearch_client=opensearch_client, login=the_first_github_login,
                                                  id=the_first_github_id)
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
