from github import GitHub
import itertools
from opensearchpy import OpenSearch
import pprint


def load_github_profile(github_tokens, opensearch_conn_infos):
    github_clients = []

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
        "size": "10000",
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
    for commit in lists["hits"]["hits"]:
        print(commit["_source"]["raw_data"]["author"]["login"])
        print(commit["_source"]["raw_data"]["committer"]["login"])

    return "load_github_profile-return"
