import copy
import itertools
import requests
import time

from opensearchpy import OpenSearch
from opensearchpy.helpers import scan as os_scan

from ..util.base import github_headers, do_get_result, HttpGetException

OPEN_SEARCH_GITHUB_PROFILE_INDEX = "github_profile"


def load_github_profile(github_tokens, opensearch_conn_infos, owner, repo):
    github_tokens_iter = itertools.cycle(github_tokens)

    opensearch_client = get_opensearch_client(opensearch_conn_infos)

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

        has_user_profile = opensearch_client.search(index=OPEN_SEARCH_GITHUB_PROFILE_INDEX,
                                                    body={
                                                        "query": {
                                                            "term": {
                                                                "login.keyword": {
                                                                    "value": github_login
                                                                }
                                                            }
                                                        }
                                                    }
                                                    )

        current_profile_list = has_user_profile["hits"]["hits"]

        now_github_profile = get_github_profile(github_tokens_iter, github_login, opensearch_conn_infos)

        if len(current_profile_list) == 0:
            opensearch_client.index(index=OPEN_SEARCH_GITHUB_PROFILE_INDEX,
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

    # github_headers.update({'Authorization': 'token %s' % next(github_tokens_iter), 'user-agent': 'Mozilla/5.0 (X11;
    # Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'})
    headers = copy.deepcopy(github_headers)
    headers.update({'Authorization': 'token %s' % next(github_tokens_iter)})
    params = {}
    req = {}
    req_session = requests.Session()
    now_github_profile = {}

    try:
        req = do_get_result(req_session, url, headers, params)
        # if req.status_code != 200:
        #     raise Exception('获取github profile 失败！')
        now_github_profile = req.json()
    except HttpGetException as hge:
        print("遇到访问github api 错误！！！")
        print("opensearch_conn_info:", opensearch_conn_infos)
        print("url:", url)
        print("status_code:", req.status_code)
        print("headers:", headers)
        print("text:", req.text)
    except TypeError as e:
        print("捕获airflow抛出的TypeError:", e)
    # finally:
    #     req.close()

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


# TODO: 传入用户的profile信息，获取用户的location、company、email，与晨琪对接
def github_profile_data_source(now_github_profile):
    import json
    now_github_profile = json.dumps(now_github_profile)

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


# TODO: 定时任务
# 更新github_profile
def add_updated_github_profiles(github_tokens, opensearch_conn_infos):
    opensearch_client = get_opensearch_client(opensearch_conn_infos)

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
        now_github_profile = get_github_profile(github_tokens_iter, existing_github_profile_login,
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
            print("用户有更新profile信息，将github上更新完的profile信息存入到opensearch中")
        else:
            print("两次的updated_at信息一致")
    return "增加更新用户信息测试"