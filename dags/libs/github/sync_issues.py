import datetime
import random
import requests
import time
import itertools

from opensearchpy import OpenSearch

from ..base_dict.opensearch_index import OPENSEARCH_INDEX_CHECK_SYNC_DATA
from ..util.github_api import GithubAPI
from ..util.log import logger
from ..util.opensearch_api import OpensearchAPI


class SyncGithubIssuesException(Exception):
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status


def sync_github_issues(github_tokens, opensearch_conn_info, owner, repo):
    logger.info("start sync_github_issues()")
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

    has_issues_check = opensearch_client.search(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA,
                                                body={
                                                    "size": 1,
                                                    "track_total_hits": True,
                                                    "query": {
                                                        "bool": {
                                                            "must": [
                                                                {
                                                                    "term": {
                                                                        "search_key.type.keyword": {
                                                                            "value": "github_issues"
                                                                        }
                                                                    }
                                                                },
                                                                {
                                                                    "term": {
                                                                        "search_key.owner.keyword": {
                                                                            "value": owner
                                                                        }
                                                                    }
                                                                },
                                                                {
                                                                    "term": {
                                                                        "search_key.repo.keyword": {
                                                                            "value": repo
                                                                        }
                                                                    }
                                                                }
                                                            ]
                                                        }
                                                    },
                                                    "sort": [
                                                        {
                                                            "search_key.update_timestamp": {
                                                                "order": "desc"
                                                            }
                                                        }
                                                    ]
                                                }
                                                )
    if len(has_issues_check["hits"]["hits"]) == 0:
        raise SyncGithubIssuesException("没有得到上次github issues 同步的时间")
    github_issues_check = has_issues_check["hits"]["hits"][0]["_source"]["github"]["issues"]

    # 生成本次同步的时间范围：同步到今天的 00:00:00
    since = datetime.datetime.fromtimestamp(github_issues_check["sync_timestamp"]).strftime('%Y-%m-%dT00:00:00Z')
    logger.info(f'sync github issues since：{since}')

    issues_numbers = []
    session = requests.Session()
    opensearch_api = OpensearchAPI()
    github_api = GithubAPI()

    for page in range(1, 10000):
        # Token sleep
        time.sleep(random.uniform(0.1, 0.2))

        req = github_api.get_github_issues(http_session=session,
                                           github_tokens_iter=github_tokens_iter,
                                           owner=owner, repo=repo, page=page, since=since)

        one_page_github_issues = req.json()

        # 提取 issues number，返回给后续task 获取 issues comments & issues timeline
        for now_github_issues in one_page_github_issues:
            issues_numbers.append(now_github_issues["number"])

        if (one_page_github_issues is not None) and len(one_page_github_issues) == 0:
            logger.info(f"sync github issues end to break:{owner}/{repo} page_index:{page}")
            break

        opensearch_api.bulk_github_issues(opensearch_client=opensearch_client,
                                          github_issues=one_page_github_issues,
                                          owner=owner, repo=repo)
        logger.info(f"success get github issues page:{owner}/{repo} page_index:{page}")

    # 建立 sync 标志
    opensearch_api.set_sync_github_issues_check(opensearch_client=opensearch_client,
                                                owner=owner, repo=repo)

    logger.info(f"issues_list:{issues_numbers}")

    # issues number，返回给后续task 获取 issues comments & issues timeline
    return issues_numbers
