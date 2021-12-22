import itertools
import requests
import time
from opensearchpy import OpenSearch
<<<<<<< HEAD


def load_github_profile(github_tokens, opensearch_conn_infos, owner, repo):
    github_tokens_iter = itertools.cycle(github_tokens)
=======
import pprint
import json


def load_github_profile(github_tokens, opensearch_conn_infos):
    github_clients = []
    print("==========================test=============================")
    for token in github_tokens:
        gh = GitHub(access_token='{token}'.format(token=token))
        github_clients.append(gh)

    github_clients_iter = itertools.cycle(github_clients)
>>>>>>> 27f7a9af4eeaa5bd092d92eb52e6172117ea8f43

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
        #
        "size": "1",  # 数量
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
<<<<<<< HEAD
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

            if len(has_user_profile["hits"]["hits"]) == 0:
                opensearch_client.index(index="github_profile",
                                        body=now_github_profile,
                                        refresh=True)
                print(now_github_profile)
            time.sleep(1)
        finally:
            r.close()
=======
    print("===============================================")
    # for commit in lists["hits"]["hits"]:
    #     print(commit["_source"]["raw_data"]["author"]["login"])
    #     print(commit["_source"]["raw_data"]["committer"]["login"])
    #     if commit["_source"]["raw_data"]["author"]["login"] == commit["_source"]["raw_data"]["committer"]["login"]:
    #         logins = [commit["_source"]["raw_data"]["author"]["login"]]
    #     else:
    #         logins = [commit["_source"]["raw_data"]["author"]["login"],
    #                   commit["_source"]["raw_data"]["committer"]["login"]]
    for commit in ['wangjianfeng7']:
        logins = [commit]
        for login in logins:
            # 查询OS中是否存在该login的profile；
            query_profile = opensearch_client.search(
                index='github_profiles',
                body={
                    "query": {
                        "term": {
                            "login": {
                                "value": login
                            }
                        }
                    }
                }
            )
            print("login:" + login)
            print("query_profile[\"hits\"][\"total\"][\"value\"]===================")
            print(query_profile["hits"]["total"]["value"])
            # 存在：结束此次循环开启下次循环；
            print(type(query_profile["hits"]["total"]["value"]))
            if query_profile["hits"]["total"]["value"] == 1:
                print("已存在")
                continue
            # 不存在：找迭代器gh查找profile，将profile存入到OS中
            print("不存在")
            gh_iter = next(github_clients_iter)
            user = gh_iter.users(login).get()
            document = json.dumps(user, indent=4)
            print(document)
            response = opensearch_client.index(
                index='github_profiles',
                body=document,
                refresh=True
            )

            print('\nAdding document:')
            print(response)
>>>>>>> 27f7a9af4eeaa5bd092d92eb52e6172117ea8f43

    return "End load_github_profile-return"
