import time
import datetime
import itertools
import requests
from opensearchpy import OpenSearch
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

    opensearch_client = OpenSearch(
        hosts=[{'host': opensearch_conn_info["HOST"], 'port': opensearch_conn_info["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_info["USER"], opensearch_conn_info["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )
    # 取得需要更新os中profile的末位id，用于判断循环更新终点
    the_last_profile = opensearch_client.search(index=OPENSEARCH_INDEX_GITHUB_PROFILE,
                                                body={
                                                    "query": {
                                                        "match_all": {}
                                                    },
                                                    "collapse": {
                                                        "field": "login.keyword"
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
    the_last_github_id = the_last_profile["hits"]["hits"][0]["_source"]["id"]

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

    existing_github_id = None
    if len(has_profile_check["hits"]["hits"]) != 0:
        existing_github_id = has_profile_check["hits"]["hits"][0]["_source"]["github"]["id"]
        if the_last_github_id == existing_github_id:
            existing_github_id = None

    # 将折叠（collapse）查询opensearch（去重排序）的结果作为查询更新的数据源
    existing_github_profiles = opensearch_client.search(
        index=OPENSEARCH_INDEX_GITHUB_PROFILE,
        body={
            "query": {
                "match_all": {}
            },
            "collapse": {
                "field": "id"
            },
            "sort": [
                {
                    "id": {
                        "order": "desc"
                    }
                },
                {
                    "updated_at": {
                        "order": "desc"
                    }
                }
            ]
        }
    )
    if not existing_github_profiles:
        logger.error("There's no existing github profiles")
        return "There's no existing github profiles"

    import json
    existing_github_profiles = json.dumps(existing_github_profiles)
    existing_github_profiles = json.loads(existing_github_profiles)["hits"]["hits"]
    next_profile_login = None

    nowTime = datetime.datetime.now() + datetime.timedelta(0)
    start_time = nowTime.strftime("%Y-%m-%d") + " {}:00:00".format(0)
    end_time = nowTime.strftime("%Y-%m-%d") + " {}:00:00".format(2)
    timeArray_start_time = time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    timeArray_end_time = time.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    github_api = GithubAPI()
    session = requests.Session()
    opensearch_api = OpensearchAPI()

    for existing_github_profile in existing_github_profiles:
        next_profile_id = existing_github_profile["_source"]["id"]
        # 获取当前时间的时间戳
        timeArray_nowTime = time.strptime(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                          "%Y-%m-%d %H:%M:%S")
        # 判断当前时间是否为查询有效时间（当日零点到凌晨两点之间）
        flag = time.mktime(timeArray_start_time) < time.mktime(timeArray_nowTime) and time.mktime(
            timeArray_nowTime) < time.mktime(timeArray_end_time)
        if not flag or the_last_github_id == next_profile_id:
            opensearch_api.set_sync_github_profiles_check(opensearch_client=opensearch_client, login=next_profile_login,
                                           id=next_profile_id)
            logger.info("Invalid Runtime")
            return "Invalid Runtime"

        if (existing_github_id is None) or (existing_github_id <= next_profile_id):
            # 获取OpenSearch中最新的profile的"updated_at"信息
            existing_github_profile_user_updated_at = existing_github_profile["_source"]["updated_at"]
            # 根据上述"login"信息获取git上的profile信息中的"updated_at1"信息
            next_profile_login = existing_github_profile["_source"]["login"]

            now_github_profile = github_api.get_github_profiles(http_session=session,
                                                                github_tokens_iter=github_tokens_iter,
                                                                id_info=next_profile_id)

            now_github_profile = json.dumps(now_github_profile)
            now_github_profile_user_updated_at = json.loads(now_github_profile)["updated_at"]

            # 将获取两次的"updated_at"信息对比
            # 一致：不作处理
            # 不一致：将新的清洗过的profile信息添加到OpenSearch中
            if existing_github_profile_user_updated_at != now_github_profile_user_updated_at:
                for key in ["name", "company", "blog", "location", "email", "hireable", "bio", "twitter_username"]:
                    if now_github_profile[key] is None:
                        now_github_profile[key]= ''
                opensearch_client.index(index=OPENSEARCH_INDEX_GITHUB_PROFILE,
                                        body=now_github_profile,
                                        refresh=True)
                logger.info(f"Success put updated {next_profile_login}'s github profiles into opensearch.")
            else:
                logger.info(f"{next_profile_login}'s github profiles of opensearch is latest.")

    return "END::sync_github_profiles"


# TODO: 传入用户的profile信息，获取用户的location、company、email，与晨琪对接
def github_profile_data_source(now_github_profile):
    import json
    now_github_profile = json.dumps(now_github_profile)
    # todo: the print statements just for testing, w can ignore them
    now_github_profile_user_company = json.loads(now_github_profile)["company"]
    if now_github_profile_user_company is not None:
        print("now_github_profile_user_company")
        print(now_github_profile_user_company)

    now_github_profile_user_location = json.loads(now_github_profile)["location"]
    if now_github_profile_user_location is not None:
        print("now_github_profile_user_location")
        print(now_github_profile_user_location)

    now_github_profile_user_email = json.loads(now_github_profile)["email"]
    if now_github_profile_user_email is not None:
        print("now_github_profile_user_email")
        print(now_github_profile_user_email)
    return "与晨琪对接哈"



