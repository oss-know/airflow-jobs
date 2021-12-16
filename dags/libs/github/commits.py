import requests
import json
import time
import itertools
import copy
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
    if not client.indices.exists("github_commits_{owner}_{repo}".format(owner=owner, repo=repo)):
        client.indices.create("github_commits_{owner}_{repo}".format(owner=owner, repo=repo))

    for page in range(9999):
        url = "https://api.github.com/repos/{owner}/{repo}/commits".format(
            owner=owner, repo=repo)
        headers = {'Authorization': 'token %s' % next(github_tokens_iter)}
        params = {'per_page': 100, 'page': page, 'since': since, 'until': until,
                  'Connection': 'keep-alive', 'Accept-Encoding': 'gzip, deflate, br', 'Accept': '*/*',
                  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36', }
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

        bulk_all_github_commits = []
        for now_commit in now_github_commits:
            has_commit = client.search(index="github_commits_{owner}_{repo}".format(owner=owner, repo=repo),
                                       body={
                                           "query": {
                                               "term": {
                                                   "raw_data.sha.keyword": {
                                                       "value": now_commit["sha"]
                                                   }
                                               }
                                           }
                                       }
                                       )
            if len(has_commit["hits"]["hits"]) == 0:
                template = {"_index": "github_commits_{owner}_{repo}".format(owner=owner, repo=repo),
                            "_source": {"search_key": {"owner": owner, "repo": repo},
                                        "raw_data": None}}
                commit_item = copy.deepcopy(template)
                commit_item["_source"]["raw_data"] = now_commit
                bulk_all_github_commits.append(commit_item)
                print("insert github commit sha:{sha}".format(sha=now_commit["sha"]))

        print("current page insert count：", len(bulk_all_github_commits))
        success, failed = OpenSearchHelpers.bulk(client=client, actions=bulk_all_github_commits)

        print("current page insert githubcommits success", success)
        print("current page insert githubcommits failed", failed)

        print("success get github:: {owner}/{repo} page_index:{page}".format(owner=owner, repo=repo, page=page))

        time.sleep(1)

    return "init_sync_github_commits END"
