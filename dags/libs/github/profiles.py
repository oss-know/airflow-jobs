import itertools
import requests
import time
from opensearchpy import OpenSearch


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

    # 查询有多少要更新的profile
    lists = opensearch_client.search(index="github_commits", body={
        "size": "10000",
        "query": {
            "bool": {"must": [
                {"term": {
                    "search_key.owner.keyword": {
                        "value": owner
                    }
                }}, {"term": {
                    "search_key.repo.keyword": {
                        "value": repo
                    }
                }}
            ]}
        }
    })

    # 循环更新profile
    all_commits_users = {}
    for commit in lists["hits"]["hits"]:
        raw_data = commit["_source"]["raw_data"]
        if (raw_data["author"] is not None) and ("author" in raw_data) and ("login" in raw_data["author"]):
            all_commits_users[raw_data["author"]["login"]] = \
                raw_data["author"]["url"]
        if (raw_data["committer"] is not None) and ("committer" in raw_data) and ("login" in raw_data["committer"]):
            all_commits_users[raw_data["committer"]["login"]] = \
                raw_data["committer"]["url"]

    for login_info in all_commits_users:
        url = "https://api.github.com/users/{login_info}".format(
            login_info=login_info)
        headers = {'Authorization': 'token %s' % next(github_tokens_iter),
                   'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'}
        # params = {}
        try:
            # print("==============开始获取profile====================")
            # print("url:", url)
            # print("headers:", headers)

            r = requests.get(url, headers=headers)
            if r.status_code != 200:
                print("opensearch_conn_info:", opensearch_conn_infos)
                print("url:", url)
                print("headers:", headers)
                print("text:", r.text)
                raise Exception('获取github profile 失败！')
            now_github_profile = r.json()

            has_user_profile = opensearch_client.search(index="github_profile",
                                                        body={
                                                            "query": {
                                                                "term": {
                                                                    "login.keyword": {
                                                                        "value": login_info
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        )

            print('has_user_profile["hits"]["hits"]+++++++++++===================')
            print(has_user_profile["hits"]["hits"])

            if len(has_user_profile["hits"]["hits"]) == 0:
                opensearch_client.index(index="github_profile",
                                        body=now_github_profile,
                                        refresh=True)
                print(now_github_profile)
            time.sleep(1)
        finally:
            r.close()

    return "End load_github_profile-return"
