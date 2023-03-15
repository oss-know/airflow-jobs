import time
import datetime
from random import randint
from dags.oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.log import logger
from oss_know.libs.util.zulip import get_data_from_opensearch
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_ZULIP_STREAM
from oss_know.libs.util.opensearch_api import OpensearchAPI


def crawl_zulip_stream(client, owner, repo, site, opensearch_conn_info):
    max_try = 20
    while(True):
        flag = 1
        try:
            result = client.get_streams()
        except Exception as e:
            flag = 0
        if flag and result["result"] == "success":
            result = result["streams"]
            break
        time.sleep(randint(10, 30))
        max_try -= 1
        if max_try == 0:
            logger.info("Fail to crawl zulip streams data.")
            if flag:
                logger.info(result["msg"])
            else:
                logger.info(e, exc_info=1)
            return
    opensearch_data = get_data_from_opensearch(OPENSEARCH_ZULIP_STREAM, owner, repo, opensearch_conn_info)
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    opensearch_api = OpensearchAPI()

    stream_exit = set()
    for item in opensearch_data:
        stream_exit.add(item["_source"]["raw_data"]["stream_id"])

    bulk_data = []
    new_stream = 0
    for item in result:
        if item["stream_id"] in stream_exit:
            continue
        bulk_data.append({
            "_index": OPENSEARCH_ZULIP_STREAM,
            "_source": {
                "owner": owner,
                "repo": repo,
                "origin": site,
                "updated_at": round(datetime.datetime.now().timestamp())
            },
            "raw_data": item
        })
        new_stream += 1

    success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                        bulk_all_data=bulk_data,
                                                        owner=owner,
                                                        repo=repo)

    logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
    logger.info(f"count:{new_stream}::{owner}/{repo}")
    bulk_data.clear()
    return
