import itertools
from loguru import logger
from . import init_profiles
import requests
from ..base_dict.opensearch_index import OPEN_SEARCH_GITHUB_PROFILE_INDEX
from ..util.github_api import GithubAPI


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


# TODO: 定时任务更新github_profile
def add_updated_github_profiles(github_tokens, opensearch_conn_infos):
    opensearch_client = init_profiles.get_opensearch_client(opensearch_conn_infos)

    github_tokens_iter = itertools.cycle(github_tokens)

    # 将折叠（collapse）查询opensearch（去重排序）的结果作为查询更新的数据源
    existing_github_profiles = opensearch_client.search(
        index=OPEN_SEARCH_GITHUB_PROFILE_INDEX,
        body={
            "query": {
                "match_all": {}
            },
            "collapse": {
                "field": "login.keyword"
            },
            "sort": [
                {
                    "updated_at": {
                        "order": "desc"
                    }
                }
            ]
        }
    )

    import json
    existing_github_profiles = json.dumps(existing_github_profiles)
    existing_github_profiles = json.loads(existing_github_profiles)["hits"]["hits"]

    for existing_github_profile in existing_github_profiles:
        # 获取OpenSearch中最新的profile的"updated_at"信息
        existing_github_profile_user_updated_at = existing_github_profile["_source"]["updated_at"]

        # 根据上述"login"信息获取git上的profile信息中的"updated_at1"信息
        existing_github_login = existing_github_profile["_source"]["login"]
        github_api = GithubAPI()
        session = requests.Session()
        now_github_profile = github_api.get_github_profiles(http_session=session, github_tokens_iter=github_tokens_iter, login_info=existing_github_login)
        # todo: test -->delete
        github_profile_data_source(now_github_profile)
        now_github_profile = json.dumps(now_github_profile)
        now_github_profile_user_updated_at = json.loads(now_github_profile)["updated_at"]

        # 将获取两次的"updated_at"信息对比
        # 一致：不作处理
        # 不一致：将新的profile信息添加到OpenSearch中
        if existing_github_profile_user_updated_at != now_github_profile_user_updated_at:
            opensearch_client.index(index=OPEN_SEARCH_GITHUB_PROFILE_INDEX,
                                    body=now_github_profile,
                                    refresh=True)
            logger.info("Put the github user's new profile into opensearch.")
    return "增加更新用户信息测试"
