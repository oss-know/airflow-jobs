import itertools
import copy
import time

import requests
from opensearchpy import OpenSearch
from opensearchpy import helpers as OpenSearchHelpers
from ..util.base import github_headers
from ..util.base import do_get_result


# from requests.adapters import HTTPAdapter
# from requests.packages.urllib3.util import Retry


def get_github_commits(session, github_tokens_iter, opensearch_conn_infos, owner, page, repo, since, until):
    url = "https://api.github.com/repos/{owner}/{repo}/commits".format(
        owner=owner, repo=repo)
    headers = copy.deepcopy(github_headers)
    headers.update({'Authorization': 'token %s' % next(github_tokens_iter)})
    params = {'per_page': 100, 'page': page, 'since': since, 'until': until}

    return do_get_result(session, url, headers, params)


def bulk_github_commits(now_github_commits, opensearch_client, owner, repo):
    bulk_all_github_commits = []
    for now_commit in now_github_commits:
        has_commit = opensearch_client.search(index="github_commits".format(owner=owner, repo=repo),
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
            template = {"_index": "github_commits".format(owner=owner, repo=repo),
                        "_source": {"search_key": {"owner": owner, "repo": repo},
                                    "raw_data": None}}
            commit_item = copy.deepcopy(template)
            commit_item["_source"]["raw_data"] = now_commit
            bulk_all_github_commits.append(commit_item)
            print("insert github commit sha:{sha}".format(sha=now_commit["sha"]))
    print("current page insert count：", len(bulk_all_github_commits))
    success, failed = OpenSearchHelpers.bulk(client=opensearch_client, actions=bulk_all_github_commits)
    print("current page insert githubcommits success", success)
    print("current page insert githubcommits failed", failed)


def init_sync_github_commits(github_tokens, opensearch_conn_infos, owner, repo, since=None, until=None):
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

    if not opensearch_client.indices.exists("github_commits".format(owner=owner, repo=repo)):
        opensearch_client.indices.create("github_commits".format(owner=owner, repo=repo))

    session = requests.sessions.Session
    for page in range(9999):
        req = get_github_commits(session, github_tokens_iter, opensearch_conn_infos, owner, page, repo, since, until)

        now_github_commits = req.json()

        if (now_github_commits is not None) and len(now_github_commits) == 0:
            print("get github commits end to break:: {owner}/{repo} page_index:{page}".format(
                owner=owner, repo=repo, page=page))
            break

        bulk_github_commits(now_github_commits, opensearch_client, owner, repo)

        print("success get github commits :: {owner}/{repo} page_index:{page}".format(owner=owner, repo=repo, page=page))

        time.sleep(1)

    return "END：：init_sync_github_commits"
