import time
import uuid
import datetime
import redis
import itertools
import requests
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_PROFILE, OPENSEARCH_INDEX_CHECK_SYNC_DATA
from oss_know.libs.util.log import logger
from opensearchpy import helpers as opensearch_helpers




STORAGE_UPDATED_HASH = 'sync_github_profiles:storage_updated_hash'
STORAGE_UPDATED_IDS_SET = 'sync_github_profiles:storage_updated_ids_set'
WORKING_UPDATED_HASH = 'sync_github_profiles:working_updated_hash'
REDIS_CLIENT_HOST = '192.168.8.107'
REDIS_CLIENT_PORT = 6379
DURATION_OF_SYNC_GITHUB_PROFILES = 3

redis_client = redis.Redis(host=REDIS_CLIENT_HOST, port=REDIS_CLIENT_PORT, db=0, decode_responses=True)


def init_storage_updated_profiles(opensearch_conn_info):

    opensearch_client = get_opensearch_client(opensearch_conn_info)
    working_updated_hash=redis_client.hgetall(WORKING_UPDATED_HASH)

    if not redis_client.hgetall(STORAGE_UPDATED_HASH) and not redis_client.hgetall(WORKING_UPDATED_HASH):
        existing_github_profiles = opensearch_helpers.scan(opensearch_client,
                                                           preserve_order=True,
                                                           index=OPENSEARCH_INDEX_GITHUB_PROFILE,
                                                           query={
                                                               "query": {
                                                                   "match_all": {}
                                                               },
                                                               "_source": ["id", "updated_at"]
                                                               ,
                                                               "sort": [
                                                                   {
                                                                       "updated_at": {
                                                                           "order": "asc"
                                                                       }
                                                                   }
                                                               ],
                                                           },
                                                           doc_type="_doc"
                                                           )

        with redis_client.pipeline(transaction=False) as p:
            for existing_github_profile in existing_github_profiles:
                if "updated_at" in existing_github_profile["_source"] and existing_github_profile["_source"]["updated_at"]\
                        and existing_github_profile['_source']['id']:
                    p.hset(name=STORAGE_UPDATED_HASH, key=existing_github_profile['_source']['id'],
                           value=existing_github_profile['_source']['updated_at'])
                    p.sadd(STORAGE_UPDATED_IDS_SET, existing_github_profile['_source']['id'])
            p.execute()

    elif working_updated_hash:
        redis_client.hmset(name=STORAGE_UPDATED_HASH, mapping=working_updated_hash)
        storage_updated_ids_set = redis_client.hkeys(WORKING_UPDATED_HASH)

        with redis_client.pipeline(transaction=False) as p:
            for storage_updated_id in storage_updated_ids_set:
                p.sadd(STORAGE_UPDATED_IDS_SET, storage_updated_id)
            p.execute()
        redis_client.delete(WORKING_UPDATED_HASH)

        

def sync_github_profiles(github_tokens, opensearch_conn_info):
    github_tokens_iter = itertools.cycle(github_tokens)
    opensearch_client = get_opensearch_client(opensearch_conn_info)
    # end_time = (datetime.datetime.now() + datetime.timedelta(hours=DURATION_OF_SYNC_GITHUB_PROFILES)).timestamp()
    end_time = (datetime.datetime.now() + datetime.timedelta(milliseconds=10000)).timestamp()
    while True:
        # pipe = redis_client.pipeline(transaction=True)
        storage_updated_id=redis_client.spop(STORAGE_UPDATED_IDS_SET)
        if not storage_updated_id:
            break
        logger.info(f'storage_updated_id:{storage_updated_id}')

        storage_updated_at = redis_client.hget(name=STORAGE_UPDATED_HASH,key=str(storage_updated_id))
        redis_client.hset(name=WORKING_UPDATED_HASH,key=str(storage_updated_id),value=str(storage_updated_at))
        redis_client.hdel(STORAGE_UPDATED_HASH, str(storage_updated_id))
        # pipe.execute()
        try:
            github_api = GithubAPI()
            session = requests.Session()
            latest_github_profile = github_api.get_github_profiles(http_session=session,
                                                                   github_tokens_iter=github_tokens_iter,
                                                                   id_info=storage_updated_id)
            latest_profile_updated_at = latest_github_profile["updated_at"]
            if storage_updated_at != latest_profile_updated_at:
                opensearch_client.index(index=OPENSEARCH_INDEX_GITHUB_PROFILE,
                                        body=latest_github_profile,
                                        refresh=True)
                logger.info(f"Success put updated {storage_updated_id}'s github profiles into opensearch.")
            else:
                logger.info(f"{storage_updated_id}'s github profiles of opensearch is latest.")
        except Exception as e:
            logger.error(f"Failed sync_github_profiles,the exception message: {e}")
        else:
            redis_client.hdel(WORKING_UPDATED_HASH,storage_updated_id)
        finally:
            if datetime.datetime.now().timestamp() > end_time:
                logger.info('The connection has timed out.')
                break

