from datetime import datetime

import requests
from opensearchpy import OpenSearch

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_RELEASES
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.util.log import logger
from oss_know.libs.util.opensearch_api import OpensearchAPI


def sync_releases(opensearch_conn_info, owner, repo, token_proxy_accommodator):
    opensearch_client = OpenSearch(
        hosts=[{'host': opensearch_conn_info["HOST"], 'port': opensearch_conn_info["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_info["USER"], opensearch_conn_info["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )
    query_body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "search_key.owner.keyword": owner,
                        }
                    },
                    {
                        "match": {
                            "search_key.repo.keyword": repo
                        }
                    }
                ]
            }
        },
        "size": 1
    }

    since = None

    query_result = opensearch_client.search(index=OPENSEARCH_INDEX_RELEASES, body=query_body)
    if query_result['hits']['hits']:
        query_body['sort'] = [
            {
                "raw_data.created_at": {
                    "order": "desc"
                }
            }
        ]
        latest_release = opensearch_client.search(index=OPENSEARCH_INDEX_RELEASES, body=query_body)
        # TODO '%Y-%m-%dT%H:%M:%SZ' should be provided as a global const value with name github_datetime_format
        latest_created_at_str = latest_release['hits']['hits'][0]['_source']['raw_data']['created_at']
        since = datetime.strptime(latest_created_at_str, '%Y-%m-%dT%H:%M:%SZ')

    session = requests.Session()
    github_api = GithubAPI()
    opensearch_api = OpensearchAPI()

    page = 1
    batch_size = 500  # 500 is quite large enough for releases
    batch = []
    total_insertions = 0

    while True:
        res = github_api.get_github_releases(session, token_proxy_accommodator, owner, repo, page, since)
        rels = res.json()

        if not rels:
            break

        if since:
            for rel in rels:
                # According to the doc below, release data is sorted by 'created_at' field:
                # https://docs.github.com/en/rest/releases/releases?apiVersion=2022-11-28#get-the-latest-release
                if datetime.strptime(rel['created_at'], '%Y-%m-%dT%H:%M:%SZ') > since:
                    batch.append(rel)
        else:
            batch += rels

        if len(batch) >= batch_size:
            succ, fail = opensearch_api.bulk_github_releases(opensearch_client, batch, owner=owner, repo=repo)
            logger.info(f'Insert {owner}/{repo} github release batch @page {page}, succ: {succ}, fail: {fail}')
            batch = []
            total_insertions += batch_size
        page += 1

    if batch:
        succ, fail = opensearch_api.bulk_github_releases(opensearch_client, batch, owner=owner, repo=repo)
        logger.info(f'Insert {owner}/{repo} github release tail batch, succ: {succ}, fail: {fail}')
        total_insertions += len(batch)
    tail_log = f'since {since.strftime("%Y-%m-%d %H:%M:%S")}' if since else ''
    logger.info(f'{owner}/{repo} releases: {total_insertions} records {tail_log} saved')
