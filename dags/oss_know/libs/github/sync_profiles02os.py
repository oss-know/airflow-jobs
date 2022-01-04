import datetime
import itertools
import requests
from opensearchpy import OpenSearch
from ..util.github_api import GithubAPI
from ..base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_PROFILE
from ..util.log import logger


class SyncGithubProfilesException(Exception):
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status


def sync_github_profiles(github_tokens,
                         opensearch_conn_info, params
                         ):
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
    # 将折叠（collapse）、login的首字母未查询条件查询opensearch（去重排序）的结果作为查询更新的数据源
    params += '*'
    existing_github_profiles = opensearch_client.search(
        index=OPENSEARCH_INDEX_GITHUB_PROFILE,
        body={
            "query": {
                "wildcard": {
                    "login.keyword": {
                        "value": params
                    }
                }
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

    if not existing_github_profiles["hits"]["hits"]:
        return "There's no logins start with "+params[0]
    import json
    existing_github_profiles = json.dumps(existing_github_profiles)
    existing_github_profiles = json.loads(existing_github_profiles)["hits"]["hits"]

    for existing_github_profile in existing_github_profiles:
        # 获取OpenSearch中最新的profile的"updated_at"信息
        existing_github_profile_user_updated_at = existing_github_profile["_source"]["updated_at"]
        # 根据上述"login"信息获取git上的profile信息中的"updated_at1"信息
        now_profile_login = existing_github_profile["_source"]["login"]
        github_api = GithubAPI()
        session = requests.Session()
        now_github_profile = github_api.get_github_profiles(http_session=session,
                                                            github_tokens_iter=github_tokens_iter,
                                                            login_info=now_profile_login)
        now_github_profile = json.dumps(now_github_profile)
        now_github_profile_user_updated_at = json.loads(now_github_profile)["updated_at"]

        # 将获取两次的"updated_at"信息对比
        # 一致：不作处理
        # 不一致：将新的profile信息添加到OpenSearch中
        if existing_github_profile_user_updated_at != now_github_profile_user_updated_at:
            opensearch_client.index(index=OPENSEARCH_INDEX_GITHUB_PROFILE,
                                    body=now_github_profile,
                                    refresh=True)
            logger.info(f"Success put updated {now_profile_login}'s github profiles into opensearch.")
        else:
            logger.info(f"{now_profile_login}'s github profiles of opensearch is latest.")

    return "END::sync_github_profiles"

