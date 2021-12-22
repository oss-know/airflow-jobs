import itertools, copy, time, datetime, requests
from opensearchpy import OpenSearch
from opensearchpy import helpers as OpenSearchHelpers
from ..util.base import github_headers, do_opensearch_bulk
from ..util.base import do_get_result

# from requests.adapters import HTTPAdapter
# from requests.packages.urllib3.util import Retry
OPENSEARCH_INDEX_CHECK_SYNC_DATA = "check_sync_data"
OPENSEARCH_INDEX_GITHUB_COMMITS = "github_commits"


def init_sync_github_commits(github_tokens,
                             opensearch_conn_info,
                             postgres_conn,
                             owner, repo, since=None, until=None):
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

    session = requests.Session()
    for page in range(9999):
        req = get_github_commits(session, github_tokens_iter, opensearch_conn_info, owner, repo, page, since, until)
        now_github_commits = req.json()

        if (now_github_commits is not None) and len(now_github_commits) == 0:
            print("get github commits end to break:: {owner}/{repo} page_index:{page}".format(
                owner=owner, repo=repo, page=page))
            break

        bulk_github_commits(now_github_commits, opensearch_client, owner, repo)

        print("success get github commits :: {owner}/{repo} page_index:{page}".format(owner=owner,
                                                                                      repo=repo,
                                                                                      page=page))

        time.sleep(1)

    set_github_init_commits_check_data(opensearch_client, owner, repo, since, until)

    return "END::init_sync_github_commits"


def get_github_commits(session, github_tokens_iter, opensearch_conn_info, owner, repo, page, since, until):
    url = "https://api.github.com/repos/{owner}/{repo}/commits".format(
        owner=owner, repo=repo)
    headers = copy.deepcopy(github_headers)
    headers.update({'Authorization': 'token %s' % next(github_tokens_iter)})
    params = {'per_page': 100, 'page': page, 'since': since, 'until': until}

    return do_get_result(session, url, headers, params)


def bulk_github_commits(now_github_commits, opensearch_client, owner, repo):
    bulk_all_github_commits = []
    for now_commit in now_github_commits:
        has_commit = opensearch_client.search(index=OPENSEARCH_INDEX_GITHUB_COMMITS,
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
            template = {"_index": OPENSEARCH_INDEX_GITHUB_COMMITS,
                        "_source": {"search_key": {"owner": owner, "repo": repo},
                                    "raw_data": None}}
            commit_item = copy.deepcopy(template)
            commit_item["_source"]["raw_data"] = now_commit
            bulk_all_github_commits.append(commit_item)
            # print("insert github commit sha:{sha}".format(sha=now_commit["sha"]))

    if len(bulk_all_github_commits) > 0:
        success, failed = do_opensearch_bulk(opensearch_client, bulk_all_github_commits)
        print("current github commits page insert count：{count},success:{success},failed:{failed}".format(
            count=len(bulk_all_github_commits), failed=failed, success=success))


# 完成owner/repo github commits 初始化后调用此方法建立初始化后下次更新的基准
def set_github_init_commits_check_data(opensearch_client,
                                       owner,
                                       repo,
                                       since,
                                       until):
    now_time = datetime.datetime.now()
    check_update_info = {
        "search_key": {
            "update_time": now_time.isoformat(),
            "update_timestamp": now_time.timestamp()
        },
        "owner": {
            "name": owner
        },
        "repo": {
            "name": repo
        },
        "github": {
            "commits": {
                "sync_timestamp": now_time.timestamp(),
                "sync_since_datetime": since,
                "sync_until_datetime": until
            }
        }
    }
    opensearch_client.index(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA,
                            body=check_update_info,
                            refresh=True)
