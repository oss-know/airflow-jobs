import random
import datetime
import requests
import time
import itertools

from opensearchpy import OpenSearch

from ..base_dict.opensearch_index import OPENSEARCH_INDEX_CHECK_SYNC_DATA, OPENSEARCH_INDEX_GITHUB_ISSUES
from ..util.github_api import GithubAPI
from ..util.opensearch_api import OpensearchAPI
from ..util.log import logger


def init_github_issues(github_tokens, opensearch_conn_infos, owner, repo, since=None):
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

    session = requests.Session()
    github_api = GithubAPI()
    opensearch_api = OpensearchAPI()
    for page in range(1, 10000):
        # Token sleep
        time.sleep(random.uniform(0.1, 0.5))

        # 获取github issues
        req = github_api.get_github_issues(http_session=session, github_tokens_iter=github_tokens_iter,
                                           owner=owner, repo=repo, page=page, since=since)
        one_page_github_issues = req.json()

        if (one_page_github_issues is not None) and len(one_page_github_issues) == 0:
            logger.info(f"init sync github issues end to break:{owner}/{repo} page_index:{page}")
            break

        # 插入一页 github isuess 到 opensearch
        opensearch_api.bulk_github_issues(opensearch_client=opensearch_client,
                                          github_issues=one_page_github_issues,
                                          owner=owner, repo=repo)

        logger.info(f"success get github issues page:{owner}/{repo} page_index:{page}")

    # 建立 sync 标志
    set_sync_github_issues_check(opensearch_client, owner, repo)


# 建立 owner/repo github issues 更新基准
def set_sync_github_issues_check(opensearch_client,
                                 owner,
                                 repo):
    now_time = datetime.datetime.now()
    check_update_info = {
        "search_key": {
            "type": "github_issues",
            "update_time": now_time.strftime('%Y-%m-%dT00:00:00Z'),
            "update_timestamp": now_time.timestamp(),
            "owner": owner,
            "repo": repo
        },
        "github": {
            "type": "github_issues",
            "owner": owner,
            "repo": repo,
            "issues": {
                "owner": owner,
                "repo": repo,
                "sync_datetime": now_time.strftime('%Y-%m-%dT00:00:00Z'),
                "sync_timestamp": now_time.timestamp()
            }
        }
    }

    opensearch_client.index(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA,
                            body=check_update_info,
                            refresh=True)
