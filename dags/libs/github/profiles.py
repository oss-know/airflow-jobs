from github import GitHub
import itertools
from opensearchpy import OpenSearch
import pprint
import json


def load_github_profile(github_tokens, opensearch_conn_infos):
    github_clients = []
    print("==========================test=============================")
    for token in github_tokens:
        gh = GitHub(access_token='{token}'.format(token=token))
        github_clients.append(gh)

    github_clients_iter = itertools.cycle(github_clients)

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
                        "value": "apache"
                    }
                }}, {"term": {
                    "search_key.repo.keyword": {
                        "value": "airflow"
                    }
                }}
            ]}
        }
    })
    # 循环更新profile
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

    return "load_github_profile-return"
