import datetime
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.log import logger
from opensearchpy import helpers
from oss_know.libs.util.opensearch_api import OpensearchAPI

GITS_MODIFY_FILES = "gits_modify_files"
OPENSEARCH_DOXYGEN_RESULT = "doxygen_result"


def get_commit_data_from_opensearch(owner, repo, opensearch_conn_info):
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    results = helpers.scan(client=opensearch_client,
                           query={
                                "track_total_hits": True,
                                "query": {
                                    "bool": {
                                        "must": [
                                            {"match": {"search_key.owner.keyword": owner}},
                                            {"match": {"search_key.repo.keyword": repo}}
                                        ]
                                    }
                                },
                                "sort":[
                                    {"raw_data.committed_date": { "order": "asc"}}
                                ]
                           },
                           index=GITS_MODIFY_FILES,
                           request_timeout=120,
                           preserve_order=True)
    return results


# def get_repo_data_from_opensearch(index, owner, repo, opensearch_conn_info):
#     opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)


def check_index(opensearch_conn_info, index):
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    index_exist = opensearch_client.indices.exists(index=index)
    if not index_exist:
        response = opensearch_client.indices.create(index=OPENSEARCH_DOXYGEN_RESULT)
        logger.info(f"Initial OPENSEARCH_DOXYGEN_RESULT.")


def put_intermediate_data_to_opensearch(owner, repo, opensearch_conn_info, lines_to_funcs):
    check_index(opensearch_conn_info, OPENSEARCH_DOXYGEN_RESULT)
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    opensearch_api = OpensearchAPI()

    bulk_data = []
    for raw_data in lines_to_funcs:
        bulk_data.append({"_index": OPENSEARCH_DOXYGEN_RESULT,
                    "_source":{
                        "search_key": {
                            "owner": owner,
                            "repo": repo,
                            "updated_at": round(datetime.datetime.now().timestamp())},
                        "raw_data": raw_data
                        }
                    })

    success, failure = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                         bulk_all_data=bulk_data,
                                                         owner=owner,
                                                         repo=repo)

    logger.info(f"sync_bulk_function_data::success:{success},failure:{failure}")
    bulk_data.clear()
    return


def get_intermediate_data_from_opensearch(owner, repo, opensearch_conn_info):
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    results = helpers.scan(client=opensearch_client,
                           query={
                                "track_total_hits": True,
                                "query": {
                                    "bool": {
                                        "must": [
                                            {"match": {"search_key.owner.keyword": owner}},
                                            {"match": {"search_key.repo.keyword": repo}}
                                        ]
                                    }
                                }
                           },
                           index=OPENSEARCH_DOXYGEN_RESULT,
                           request_timeout=120,
                           preserve_order=True)
    return results
