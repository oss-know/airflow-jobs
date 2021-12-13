import requests
import json
import itertools
from opensearchpy import OpenSearch
from opensearchpy import helpers as OpenSearchHelpers


def sync_github_commits(github_tokens, opensearch_conn_infos, owner, repo, since=None, until=None):
    github_tokens_iter = itertools.cycle(github_tokens)
    client = OpenSearch(
        hosts=[{'host': opensearch_conn_infos["HOST"], 'port': opensearch_conn_infos["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_infos["USER"], opensearch_conn_infos["PW"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )
    for page in range(1):
        url = "https://api.github.com/repos/{owner}/{repo}/commits".format(
            owner=owner, repo=repo)
        headers = {'Authorization': 'token %s' % next(github_tokens_iter)}
        params = {'per_page': 100, 'page': page, 'since': since, 'until': until}
        r = requests.get(url, headers=headers, params=params)

        if r.status_code != 200:
            print("url")
            print(url)
            print("headers")
            print(headers)
            print("params")
            print(params)
            raise Exception('获取github commits 失败！')

        now_github_commits = r.json()

        if (now_github_commits is not None) and (len(now_github_commits) > 0):
            bulk_all_github_commits = []
            template = {"_index": "github_commits",
                        "_source": None}
            for now_commit in now_github_commits:
                commit_item = template.copy()
                commit_item["_source"] = now_commit
                bulk_all_github_commits.append(commit_item)
            success, failed = OpenSearchHelpers.bulk(client=client, actions=bulk_all_github_commits)

        print("sync_github_commits_success", success)
        print("sync_github_commits_failed", failed)

    return
