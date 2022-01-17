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

redis_client = redis.Redis(host='192.168.8.107', port=6379, db=0, decode_responses=True)


def get_need_updated_profiles(opensearch_conn_info):
    opensearch_client = get_opensearch_client(opensearch_conn_info)
    failed_updated_hash=redis_client.hgetall("sync_github_profiles:failed_updated_hash")
    if failed_updated_hash or redis_client.hgetall("sync_github_profiles:failed_updated_hash"):
        if failed_updated_hash:
            redis_client.hmset(name='sync_github_profiles:need_updated_hash', mapping=failed_updated_hash)
            need_updated_ids_set = redis_client.hkeys('sync_github_profiles:failed_updated_hash')
            with redis_client.pipeline(transaction=False) as p:
                for need_updated_id in need_updated_ids_set:
                    p.sadd('sync_github_profiles:need_updated_ids', need_updated_id)
                p.execute()
            redis_client.delete('sync_github_profiles:failed_updated_hash')
    else:
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
                if "updated_at" not in existing_github_profile["_source"] or not existing_github_profile['_source']['id'] :
                    continue
                else:
                    p.hset(name='sync_github_profiles:need_updated_hash', key=existing_github_profile['_source']['id'],
                           value=existing_github_profile['_source']['updated_at'])
                    p.sadd('sync_github_profiles:need_updated_ids', existing_github_profile['_source']['id'])
            p.execute()

def sync_github_profiles(github_tokens, opensearch_conn_info):
    github_tokens_iter = itertools.cycle(github_tokens)
    opensearch_client = get_opensearch_client(opensearch_conn_info)
    # end_time = (datetime.datetime.now() + datetime.timedelta(hours=3)).timestamp()
    end_time = (datetime.datetime.now() + datetime.timedelta(milliseconds=10000)).timestamp()
    while True:
        identifier = acquire_lock('resource')
        need_updated_id=redis_client.spop('sync_github_profiles:need_updated_ids')
        if not need_updated_id:
            continue
        logger.info(f'need_updated_id:{need_updated_id}')
        need_updated_updated_at = redis_client.hget('sync_github_profiles:need_updated_hash',need_updated_id)
        redis_client.hset(name='sync_github_profiles:failed_updated_hash',key=need_updated_id,value=need_updated_updated_at)
        redis_client.hdel('sync_github_profiles:need_updated_hash', need_updated_id)
        release_lock('resource', identifier)
        github_api = GithubAPI()
        session = requests.Session()
        latest_github_profile = github_api.get_github_profiles(http_session=session,
                                                               github_tokens_iter=github_tokens_iter,
                                                               id_info=need_updated_id)
        latest_profile_updated_at = latest_github_profile["updated_at"]

        if need_updated_updated_at != latest_profile_updated_at:
            opensearch_client.index(index=OPENSEARCH_INDEX_GITHUB_PROFILE,
                                    body=latest_github_profile,
                                    refresh=True)
            logger.info(f"Success put updated {need_updated_id}'s github profiles into opensearch.")
        else:
            logger.info(f"{need_updated_id}'s github profiles of opensearch is latest.")
        redis_client.hdel('sync_github_profiles:failed_updated_hash',need_updated_id)
        if datetime.datetime.now().timestamp() > end_time:
            logger.info('The connection has timed out.')
            break


def acquire_lock(lock_name, acquire_time=10, time_out=10):
    """获取一个分布式锁"""
    identifier = str(uuid.uuid4())
    end = time.time() + acquire_time
    lock = "string:lock:" + lock_name
    while time.time() < end:
        if redis_client.setnx(lock, identifier):
            # 给锁设置超时时间, 防止进程崩溃导致其他进程无法获取锁
            redis_client.expire(lock, time_out)
            return identifier
        elif not redis_client.ttl(lock):
            redis_client.expire(lock, time_out)
        time.sleep(0.001)
    return False

def release_lock(lock_name, identifier):
    """通用的锁释放函数"""
    lock = "string:lock:" + lock_name
    pip = redis_client.pipeline(True)
    while True:
        try:
            pip.watch(lock)
            lock_value = redis_client.get(lock)
            if not lock_value:
                return True

            if lock_value == identifier:
                pip.multi()
                pip.delete(lock)
                pip.execute()
                return True
            pip.unwatch()
            break
        except redis.exceptions.WatchError:
            pass
    return False

