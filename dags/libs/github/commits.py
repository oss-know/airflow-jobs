import requests
import json
import itertools
from opensearchpy import OpenSearch
from opensearchpy import helpers as OpenSearchHelpers


def init_sync_github_commits(github_tokens, opensearch_conn_infos, owner, repo, since=None, until=None):
    github_tokens_iter = itertools.cycle(github_tokens)

    client = OpenSearch(
        hosts=[{'host': opensearch_conn_infos["HOST"], 'port': opensearch_conn_infos["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_infos["USER"], opensearch_conn_infos["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )
    all_github_commits = []
    for page in range(9999):
        url = "https://api.github.com/repos/{owner}/{repo}/commits".format(
            owner=owner, repo=repo)
        headers = {'Authorization': 'token %s' % next(github_tokens_iter)}
        params = {'per_page': 100, 'page': page, 'since': since, 'until': until}
        r = requests.get(url, headers=headers, params=params)

        if r.status_code != 200:
            print("opensearch_conn_info:", opensearch_conn_infos)
            print("url:", url)
            print("headers:", headers)
            print("params:", params)
            print("text:", r.text)
            raise Exception('获取github commits 失败！')

        now_github_commits = r.json()
        if (now_github_commits is not None) and len(now_github_commits) == 0:
            break
        all_github_commits = all_github_commits + now_github_commits

        print("success get github {owner}/{repo}/{page}:".format(owner=owner, repo=repo, page=page))

    client.delete_by_query(index="github_commits",
                           body={
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

    if (all_github_commits is not None) and (len(all_github_commits) > 0):
        bulk_all_github_commits = []
        template = {"_index": "github_commits",
                    "_source": {"search_key": {"owner": owner, "repo": repo},
                                "raw_data": None}}
        for now_commit in all_github_commits:
            commit_item = template.copy()
            commit_item["_source"]["raw_data"] = now_commit
            bulk_all_github_commits.append(commit_item)

        # 批量插入数据
        success, failed = OpenSearchHelpers.bulk(client=client, actions=bulk_all_github_commits)

        print("init_sync_github_commits_success", success)
        print("init_sync_github_commits_failed", failed)

    return
