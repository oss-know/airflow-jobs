import datetime
import itertools
import requests
from opensearchpy import OpenSearch
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_PROFILE, OPENSEARCH_INDEX_CHECK_SYNC_DATA
from oss_know.libs.util.log import logger


class SyncGithubProfilesException(Exception):
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status


def sync_github_profiles(github_tokens,
                         opensearch_conn_info,
                         ):
    print("==================================1231test=======================================")
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

    # 取得上次更新github profile 的位置
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
    print("======================has_profile_check===========================")
    print(has_profile_check)
    existing_github_id = None
    if len(has_profile_check["hits"]["hits"]) != 0:
        print("=====================len(has_profile_check[\"hits\"][\"hits\"]) != 0==================================")
        existing_github_id = has_profile_check["hits"]["hits"][0]["_source"]["github"]["id"]
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
    print("++++++++===============existing_github_profiles==================================")
    print(existing_github_profiles)
    import json
    existing_github_profiles = json.dumps(existing_github_profiles)
    existing_github_profiles = json.loads(existing_github_profiles)["hits"]["hits"]

    next_profile_id = None
    next_profile_login = None

    for existing_github_profile in existing_github_profiles:
        next_profile_id = existing_github_profile["_source"]["id"]
        print("============================next_profile_id=================================")
        print(next_profile_id)
        if (existing_github_id is None) or (existing_github_id <= next_profile_id):
            # 获取OpenSearch中最新的profile的"updated_at"信息
            existing_github_profile_user_updated_at = existing_github_profile["_source"]["updated_at"]
            # 根据上述"login"信息获取git上的profile信息中的"updated_at1"信息
            next_profile_login = existing_github_profile["_source"]["login"]
            github_api = GithubAPI()
            session = requests.Session()
            now_github_profile = github_api.get_github_profiles(http_session=session,
                                                                github_tokens_iter=github_tokens_iter,
                                                                login_info=next_profile_login)

            now_github_profile = json.dumps(now_github_profile)
            now_github_profile_user_updated_at = json.loads(now_github_profile)["updated_at"]

            # 将获取两次的"updated_at"信息对比
            # 一致：不作处理
            # 不一致：将新的profile信息添加到OpenSearch中
            if existing_github_profile_user_updated_at != now_github_profile_user_updated_at:
                opensearch_client.index(index=OPENSEARCH_INDEX_GITHUB_PROFILE,
                                        body=now_github_profile,
                                        refresh=True)
                logger.info(f"Success put updated {next_profile_login}'s github profiles into opensearch.")
            else:
                logger.info(f"{next_profile_login}'s github profiles of opensearch is latest.")
    set_sync_github_profiles_check(opensearch_client=opensearch_client, login=next_profile_login, id=next_profile_id)

    return "END::sync_github_profiles"


# 建立 owner/repo github issues 更新基准
def set_sync_github_profiles_check(opensearch_client, login, id):
    now_time = datetime.datetime.now()
    check_update_info = {
        "search_key": {
            "type": "github_profiles",
            "update_time": now_time.strftime('%Y-%m-%dT00:00:00Z'),
            "update_timestamp": now_time.timestamp(),
            "login": login,
            "id": id,
        },
        "github": {
            "type": "github_profiles",
            "login": login,
            "id": id,
            "profiles": {
                "login": login,
                "id": id,
                "sync_datetime": now_time.strftime('%Y-%m-%dT00:00:00Z'),
                "sync_timestamp": now_time.timestamp()
            }
        }
    }

    opensearch_client.index(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA,
                            body=check_update_info,
                            refresh=True)
