import random

import requests
import time
import itertools
import copy

from opensearchpy import OpenSearch
from opensearchpy import helpers as opensearch_helpers

from .init_issues_timeline import get_github_issues_timeline
from ..base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES, OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE
from ..util.log import logger


def sync_github_issues_timeline(github_tokens, opensearch_conn_info, owner, repo, issues_numbers):
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
    http_session = requests.Session()
    for number in issues_numbers:
        for page in range(1, 10000):
            # Token sleep
            time.sleep(random.uniform(0.1, 0.5))

            req = get_github_issues_timeline(http_session=http_session, github_tokens_iter=github_tokens_iter,
                                             owner=owner, repo=repo, number=number, page=page)
            one_page_github_issues_timeline = req.json()

            if (one_page_github_issues_timeline is not None) and len(
                    one_page_github_issues_timeline) == 0:
                logger.info(f"sync github issues end to break:{owner}/{repo} page_index:{page}")
                break

            bulk_github_issues_timeline(one_page_github_issues_timeline,
                                        opensearch_client, owner, repo, number)

            logger.info(f"success get github issues page:{owner}/{repo} page_index:{page}")


