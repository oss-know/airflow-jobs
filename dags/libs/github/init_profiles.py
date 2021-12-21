import itertools
import requests
import time
from opensearchpy import OpenSearch
from opensearchpy.helpers import scan as os_scan
from ..util.base import github_headers, do_get_result, GithubGetException

OPEN_SEARCH_GITHUB_PROFILE_INDEX = "github_profile"


def load_github_profile(github_tokens, opensearch_conn_infos, owner, repo):
    github_tokens_iter = itertools.cycle(github_tokens)

    opensearch_client = OpenSearch(
        hosts=[{'host': opensearch_conn_infos["HOST"], 'port': opensearch_conn_infos["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_infos["USER"], opensearch_conn_infos["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )

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

    return "End::load_github_profile"


def get_github_profile(github_tokens_iter, login_info, opensearch_conn_infos):
    url = "https://api.github.com/users/{login_info}".format(
        login_info=login_info)
    github_headers.update({'Authorization': 'token %s' % next(github_tokens_iter),
                           'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'})
    try:
        req_session = requests.Session()
        req = do_get_result(req_session, url, headers=github_headers)
        if req.status_code != 200:
            raise Exception('获取github profile 失败！')
        now_github_profile = req.json()
    except GithubGetException:
        print("遇到访问github api 错误！！！")
        print("opensearch_conn_info:", opensearch_conn_infos)
        print("url:", url)
        print("status_code:", req.status_code)
        print("headers:", github_headers)
        print("text:", req.text)
    finally:
        req.close()

    return now_github_profile
