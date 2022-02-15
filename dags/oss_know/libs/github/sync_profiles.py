import datetime
import itertools
import requests
from oss_know.libs.util.base import get_opensearch_client, get_redis_client, \
    infer_country_company_geo_insert_into_profile
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_PROFILE
from oss_know.libs.base_dict.redis import STORAGE_HASH, STORAGE_IDS_SET, WORKING_HASH
from oss_know.libs.util.log import logger
from opensearchpy import helpers as opensearch_helpers


def init_storage_pipeline(opensearch_conn_info, redis_client_info):
    '''Put GitHub profiles that need to be updated into dict named sync_github_profiles:storage_updated_hash of redis.'''
    opensearch_client = get_opensearch_client(opensearch_conn_info)
    redis_client = get_redis_client(redis_client_info)
    working_updated_hash = redis_client.hgetall(WORKING_HASH)

    if not redis_client.hgetall(STORAGE_HASH) and not redis_client.hgetall(WORKING_HASH):
        existing_github_profiles = opensearch_helpers.scan(opensearch_client,
                                                           scroll="30m",
                                                           preserve_order=True,
                                                           index=OPENSEARCH_INDEX_GITHUB_PROFILE,
                                                           query={
                                                               "query": {
                                                                   "match_all": {}
                                                               },
                                                               "_source": ["raw_data.id", "raw_data.updated_at"],
                                                               "sort": [
                                                                   {
                                                                       "raw_data.updated_at": {
                                                                           "order": "asc"
                                                                       }
                                                                   }
                                                               ]
                                                           },
                                                           doc_type="_doc"
                                                           )
        with redis_client.pipeline(transaction=False) as p:
            for existing_github_profile in existing_github_profiles:
                if existing_github_profile["_source"]['raw_data']["updated_at"] != '1970-01-01T00:00:00Z' \
                        and existing_github_profile['_source']['raw_data']['id']:
                    p.hset(name=STORAGE_HASH, key=existing_github_profile['_source']['raw_data']['id'],
                           value=existing_github_profile['_source']['raw_data']['updated_at'])
                    p.sadd(STORAGE_IDS_SET, existing_github_profile['_source']['raw_data']['id'])
            p.execute()

    elif working_updated_hash:
        redis_client.hmset(name=STORAGE_HASH, mapping=working_updated_hash)
        storage_updated_ids_set = redis_client.hkeys(WORKING_HASH)

        with redis_client.pipeline(transaction=False) as p:
            for storage_updated_id in storage_updated_ids_set:
                p.sadd(STORAGE_IDS_SET, storage_updated_id)
            p.execute()
        redis_client.delete(WORKING_HASH)


def sync_github_profiles(github_tokens, opensearch_conn_info, redis_client_info, duration_of_sync_github_profiles):
    '''Update GitHub profiles in dict named sync_github_profiles:storage_updated_hash of redis.'''
    github_tokens_iter = itertools.cycle(github_tokens)
    opensearch_client = get_opensearch_client(opensearch_conn_info)
    redis_client = get_redis_client(redis_client_info)
    end_time = (datetime.datetime.now() + datetime.timedelta(
        seconds=duration_of_sync_github_profiles["seconds"])).timestamp()
    while True:
        lua_cmd = redis_client.register_script(
            """
            local storage_id = redis.pcall('spop',KEYS[1])
            if storage_id == "nil" then
                return storage_id
            end
            local updated_at =  redis.pcall('hget',KEYS[2],storage_id)
            redis.pcall('hset',KEYS[3],storage_id,updated_at)
            redis.pcall('hdel',KEYS[2],storage_id)
            return storage_id
            """
        )
        storage_id = lua_cmd(keys=[STORAGE_IDS_SET, STORAGE_HASH, WORKING_HASH])
        if not storage_id:
            logger.info("There's no more id in storage_ids_set.")
            break
        logger.info(f'storage_id:{storage_id}')
        storage_updated_at = redis_client.hget(WORKING_HASH, storage_id)
        try:
            github_api = GithubAPI()
            session = requests.Session()
            latest_github_profile = github_api.get_latest_github_profile(http_session=session,
                                                                         github_tokens_iter=github_tokens_iter,
                                                                         user_id=storage_id)
            latest_profile_updated_at = latest_github_profile["updated_at"]
            if storage_updated_at != latest_profile_updated_at:
                infer_country_company_geo_insert_into_profile(latest_github_profile)
                opensearch_client.index(index=OPENSEARCH_INDEX_GITHUB_PROFILE,
                                        body={"search_key": {
                                            'updated_at': (datetime.datetime.now().timestamp() * 1000)},
                                            "raw_data": latest_github_profile},
                                        refresh=True)
                logger.info(f"Success put updated {storage_id}'s github profiles into opensearch.")
            else:
                logger.info(f"{storage_id}'s github profiles of opensearch is latest.")
        except Exception as e:
            logger.error(f"Failed sync_github_profiles,the exception message: {e}")
        else:
            redis_client.hdel(WORKING_HASH, storage_id)
        finally:
            if datetime.datetime.now().timestamp() > end_time:
                logger.info('The connection has timed out.')
                break
