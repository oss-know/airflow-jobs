import requests
import time
import itertools
import copy
from opensearchpy import OpenSearch
from opensearchpy import helpers as OpenSearchHelpers
from ..util.base import github_headers, do_get_result

OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS = "github_pull_requests"


def init_sync_github_pull_requests(github_tokens, opensearch_conn_info, owner, repo, since=None):
    github_tokens_iter = itertools.cycle(github_tokens)

    opensearch_client = OpenSearch(
        hosts=[{'host': opensearch_conn_info["HOST"], 'port': opensearch_conn_info["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_info["USER"], opensearch_conn_info["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )

    # 不要在dag or task里面 创建index 会有并发异常！！！
    # if not opensearch_client.indices.exists("github_issues"):
    #     opensearch_client.indices.create("github_issues")

    # 由于需要初始化幂等要求，在重新初始化前删除对应owner/repo的所有数据
    del_result = opensearch_client.delete_by_query(index=OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS,
                                                   body={
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
                                                   })
    print("DELETE github pull_requests result:", del_result)

    # 获取指定owner/page 的 pull_requests
    session = requests.Session()
    for page in range(1, 10000):
        # Token sleep
        time.sleep(1)

        req = get_github_pull_requests(session, github_tokens_iter, opensearch_conn_info, owner, page, repo, since)
        one_page_github_pull_requests = req.json()

        if (one_page_github_pull_requests is not None) and len(one_page_github_pull_requests) == 0:
            print("init sync github pull_requests end to break:{owner}/{repo} page_index:{page}".format(
                owner=owner, repo=repo, page=page))
            break

        bulk_github_pull_requests(one_page_github_pull_requests, opensearch_client, owner, repo)

        print(
            "success get github pull_requests page:{owner}/{repo} page_index:{page}".format(owner=owner,
                                                                                            repo=repo,
                                                                                            page=page))


def get_github_pull_requests(session, github_tokens_iter, opensearch_conn_infos, owner, page, repo, since):
    url = "https://api.github.com/repos/{owner}/{repo}/pulls".format(
        owner=owner, repo=repo)
    headers = copy.deepcopy(github_headers)
    headers.update({'Authorization': 'token %s' % next(github_tokens_iter)})
    params = {'state': 'all', 'per_page': 100, 'page': page, 'since': since}
    res = do_get_result(session, url, headers, params)
    return res


def bulk_github_pull_requests(now_github_pull_requests, opensearch_client, owner, repo):
    bulk_all_github_pull_requests = []

    for now_pull_requests in now_github_pull_requests:
        template = {"_index": OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS,
                    "_source": {"search_key": {"owner": owner, "repo": repo},
                                "raw_data": None}}
        pull_requests_item = copy.deepcopy(template)
        pull_requests_item["_source"]["raw_data"] = now_pull_requests
        bulk_all_github_pull_requests.append(pull_requests_item)
        print("add init sync github pull_requests number:{number}".format(number=now_pull_requests["number"]))

    success, failed = OpenSearchHelpers.bulk(client=opensearch_client, actions=bulk_all_github_pull_requests)
    print("now page:{size} sync github pull_requests success:{success} & failed:{failed}".format(
        size=len(bulk_all_github_pull_requests), success=success, failed=failed))

    return success, failed
