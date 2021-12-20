import itertools
import requests
import time
from opensearchpy import OpenSearch
from opensearchpy.helpers import scan as os_scan
from ..util.base import github_headers


def load_github_profile(github_tokens, opensearch_conn_infos, owner, repo):
    github_tokens_iter = itertools.cycle(github_tokens)

    opensearch_client = get_opensearch_client(opensearch_conn_infos)

    # 查询owner+repo所有github commits记录用来提取github author和committer
    res = os_scan(client=opensearch_client, query={
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
        }
    }, index='github_commits', doc_type='_doc', timeout='1m')

    # 对github author 和 committer 去重
    all_commits_users = {}
    for commit in res:
        raw_data = commit["_source"]["raw_data"]
        if (raw_data["author"] is not None) and ("author" in raw_data) and ("login" in raw_data["author"]):
            all_commits_users[raw_data["author"]["login"]] = \
                raw_data["author"]["url"]
        if (raw_data["committer"] is not None) and ("committer" in raw_data) and ("login" in raw_data["committer"]):
            all_commits_users[raw_data["committer"]["login"]] = \
                raw_data["committer"]["url"]

    # 获取github profile
    for github_login in all_commits_users:
        time.sleep(1)

        has_user_profile = opensearch_client.search(index="github_profile",
                                                    body={
                                                        "query": {
                                                            "term": {
                                                                "login.keyword": {
                                                                    "value": github_login
                                                                }
                                                            }
                                                        },
                                                        "size": 10
                                                    }
                                                    )

        current_profile_list = has_user_profile["hits"]["hits"]

        now_github_profile = get_github_profile(github_tokens_iter, github_login, opensearch_conn_infos)

        if len(current_profile_list) == 0:
            opensearch_client.index(index="github_profile",
                                    body=now_github_profile,
                                    refresh=True)
            import json
            now_github_profile = json.dumps(now_github_profile, indent=4)
            now_github_user_id = json.loads(now_github_profile)["id"]
            opensearch_client.index(index="github_profile_userid",
                                    body={"github_user_id": now_github_user_id},
                                    refresh=True)

    return "End::load_github_profile"


def get_github_profile(github_tokens_iter, login_info, opensearch_conn_infos):
    url = "https://api.github.com/users/{login_info}".format(
        login_info=login_info)
    github_headers.update({'Authorization': 'token %s' % next(github_tokens_iter),
                           'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                         'Chrome/96.0.4664.110 Safari/537.36'})
    try:
        req = requests.get(url, headers=github_headers)
        if req.status_code != 200:
            print("opensearch_conn_info:", opensearch_conn_infos)
            print("url:", url)
            print("headers:", github_headers)
            print("text:", req.text)
            raise Exception('获取github profile 失败！')
        now_github_profile = req.json()
    finally:
        req.close()
    return now_github_profile


# 连接OpenSearch
def get_opensearch_client(opensearch_conn_infos):
    opensearch_client = OpenSearch(
        hosts=[{'host': opensearch_conn_infos["HOST"], 'port': opensearch_conn_infos["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_infos["USER"], opensearch_conn_infos["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )
    return opensearch_client


# TODO : 定时任务
# TODO： 将查询更新的数据源从建立id新表变成折叠（collapse）查询opensearch（去重排序）
# 更新github_profile
def add_updated_github_profiles(github_tokens, opensearch_conn_infos):
    opensearch_client = get_opensearch_client(opensearch_conn_infos)
    github_profile_userids = opensearch_client.search(index="github_profile_userid",
                                                      body={
                                                          "query": {
                                                              "match_all": {}
                                                          }
                                                      }
                                                      )

    github_tokens_iter = itertools.cycle(github_tokens)
    github_profile_userids = github_profile_userids["hits"]["hits"]
    for github_profile_userid in github_profile_userids:
        # 以id为查询条件获取OpenSearch中的最新profile信息
        existing_github_profile = opensearch_client.search(
            index='github_profile',
            body={
                "query": {
                    "term": {
                        "id": {
                            "value": github_profile_userid["_source"]["github_user_id"]
                        }
                    }
                },
                "size": 1,
                "sort": {
                    "updated_at": {
                        "order": "desc"
                    }
                }
            }
        )

        # 获取OpenSearch中最新的profile的"updated_at"信息
        import json
        existing_github_profile = json.dumps(existing_github_profile)
        existing_github_profile_user_updated_at = json.loads(existing_github_profile)["hits"]["hits"][0]["_source"]["updated_at"]

        # 根据上述"login"信息获取git上的profile信息中的"updated_at1"信息
        existing_github_profile_user = json.loads(existing_github_profile)["hits"]["hits"][0]["_source"]["login"]
        now_github_profile = get_github_profile(github_tokens_iter, existing_github_profile_user, opensearch_conn_infos)
        now_github_profile = json.dumps(now_github_profile)
        now_github_profile_user_updated_at = json.loads(now_github_profile)["updated_at"]

        # 将获取两次的"updated_at"信息对比
        # 一致：不作处理
        # 不一致：将新的profile信息添加到OpenSearch中
        if existing_github_profile_user_updated_at != now_github_profile_user_updated_at:
            opensearch_client.index(index="github_profile",
                                    body=now_github_profile,
                                    refresh=True)
        else:
            print("两次的updated_at信息一致")
    return "增加更新用户信息测试"
