import itertools
import time
from opensearchpy.helpers import scan as os_scan
from loguru import logger
from . import init_profile_commen

OPEN_SEARCH_GITHUB_PROFILE_INDEX = "github_profile"


def load_github_profile(github_tokens, opensearch_conn_infos, owner, repo):
    """Get GitHub user's profile from GitHub commit and put it into opensearch if it is not in opensearch."""

    github_tokens_iter = itertools.cycle(github_tokens)

    opensearch_client = init_profile_commen.get_opensearch_client(opensearch_conn_infos)

    # 查询owner+repo所有github commits记录用来提取github author和committer
    res = os_scan(client=opensearch_client, index='github_commits',
                  query={
                      "track_total_hits": True,
                      "query": {
                          "bool": {"must": [
                              {"term": {
                                  "search_key.owner.keyword": {
                                      "value": owner
                                  }
                              }},
                              {"term": {
                                  "search_key.repo.keyword": {
                                      "value": repo
                                  }
                              }}
                          ]}
                      },
                      "size": 10
                  }, doc_type='_doc', timeout='10m')

    # 对github author 和 committer 去重
    all_commits_users = {}
    all_commits_users_set = set([])

    for commit in res:
        raw_data = commit["_source"]["raw_data"]
        if (raw_data["author"] is not None) and ("author" in raw_data) and ("login" in raw_data["author"]):
            all_commits_users[raw_data["author"]["login"]] = \
                raw_data["author"]["url"]
            all_commits_users_set.add(raw_data["author"]["login"])
        if (raw_data["committer"] is not None) and ("committer" in raw_data) and ("login" in raw_data["committer"]):
            all_commits_users[raw_data["committer"]["login"]] = \
                raw_data["committer"]["url"]
            all_commits_users_set.add(raw_data["committer"]["login"])

    # 获取github profile
    # init_profile_commen.put_profile_into_opensearch(opensearch_client, all_commits_users_set,
    #                                                 OPEN_SEARCH_GITHUB_PROFILE_INDEX, github_tokens_iter,
    #                                                 opensearch_conn_infos)
    init_profile_dict = {'opensearch_client': opensearch_client, 'logins': all_commits_users_set,
                         'OPEN_SEARCH_GITHUB_PROFILE_INDEX': OPEN_SEARCH_GITHUB_PROFILE_INDEX,
                         'github_tokens_iter': github_tokens_iter, 'opensearch_conn_infos': opensearch_conn_infos}
    logger.info(load_github_profile.__doc__)

    return init_profile_dict


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
    opensearch_client = init_profile_commen.get_opensearch_client(opensearch_conn_infos)

    github_tokens_iter = itertools.cycle(github_tokens)

    # 将折叠（collapse）查询opensearch（去重排序）的结果作为查询更新的数据源
    existing_github_profiles = opensearch_client.search(
        index='github_profile',
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
        existing_github_profile_login = existing_github_profile["_source"]["login"]
        now_github_profile = init_profile_commen.get_github_profile(github_tokens_iter, existing_github_profile_login,
                                                                    opensearch_conn_infos)
        # todo: test -->delete
        github_profile_data_source(now_github_profile)

        now_github_profile = json.dumps(now_github_profile)
        now_github_profile_user_updated_at = json.loads(now_github_profile)["updated_at"]

        # 将获取两次的"updated_at"信息对比
        # 一致：不作处理
        # 不一致：将新的profile信息添加到OpenSearch中
        if existing_github_profile_user_updated_at != now_github_profile_user_updated_at:
            opensearch_client.index(index="github_profile",
                                    body=now_github_profile,
                                    refresh=True)
            logger.info("Put the github user's new profile into opensearch.")
    return "增加更新用户信息测试"
