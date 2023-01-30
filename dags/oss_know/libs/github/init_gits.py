import copy
import datetime
import os
import shutil
import uuid
from time import sleep

from git import Repo
from oss_know.libs.util.log import logger

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_GIT_RAW, OPENSEARCH_INDEX_CHECK_SYNC_DATA
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.opensearch_api import OpensearchAPI


def timestamp_to_utc(timestamp):
    # 10位时间戳
    return datetime.datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%dT%H:%M:%SZ")


# 用于记录上一次更新的点
def sync_git_check_update_info(opensearch_client, owner, repo, head_commit):
    now_time = datetime.datetime.now()
    check_update_info = {
        "search_key": {
            "type": "git_commits",
            "update_time": now_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "update_timestamp": now_time.timestamp(),
            "owner": owner,
            "repo": repo
        },
        "git": {
            "type": "git_commits",
            "owner": owner,
            "repo": repo,
            "commits": {
                "sync_timestamp": now_time.timestamp(),
                "sync_commit_sha": head_commit
            }
        }
    }
    response = opensearch_client.index(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA, body=check_update_info)
    logger.info(response)


def delete_old_data(owner, repo, client, sync=True):
    query_body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"search_key.owner.keyword": {"value": owner}}},
                    {"term": {"search_key.repo.keyword": {"value": repo}}}
                ],
                "must_not": [
                    {
                        "exists": {
                            "field": "search_key.place_holder_uuid"
                        }
                    }
                ]
            }
        }
    }
    response = client.delete_by_query(index=OPENSEARCH_GIT_RAW, body=query_body)
    logger.info(f"Deleting old data of {owner}/{repo}, response: {response}")

    # Make sure all the docs are deleted with a dummy polling
    # TODO Maybe there are better solutions with OpenSearch's mechanism?
    if sync:
        for i in range(20):
            sleep(0.5)
            res = client.search(index=OPENSEARCH_GIT_RAW, body=query_body)
            if res['hits']['total']['value'] == 0:
                break


# Generate a place-holder doc for (owner, repo) in gits index
# Since the daily-gits-sync read uniq (owner, repo) pair and generate the tasks topology
# The place-holder doc makes sure the topology won't change even we have to delete all (owner, repo)'s
# data docs
def insert_flag_doc(client, owner, repo):
    _uuid = uuid.uuid1().hex
    doc_body = {
        "search_key": {
            "owner": owner,
            "repo": repo,
            "place_holder_uuid": _uuid
        }
    }
    client.index(index=OPENSEARCH_GIT_RAW, body=doc_body)

    search_body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"search_key.place_holder_uuid": _uuid}},
                    {"term": {"search_key.owner.keyword": owner}},
                    {"term": {"search_key.repo.keyword": repo}}
                ]
            }
        }
    }
    for i in range(20):
        sleep(0.5)
        res = client.search(index=OPENSEARCH_GIT_RAW, body=search_body)
        if res['hits']['total']['value'] != 0:
            break
    return _uuid


def remove_flag_doc(client, owner, repo, doc_uuid):
    search_body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"search_key.place_holder_uuid": doc_uuid}},
                    {"term": {"search_key.owner.keyword": owner}},
                    {"term": {"search_key.repo.keyword": repo}}
                ]
            }
        }
    }
    res = client.delete_by_query(index=OPENSEARCH_GIT_RAW, body=search_body)
    logger.info(f"Deleting place holder doc response: {res}")
    for i in range(20):
        sleep(0.5)
        res = client.search(index=OPENSEARCH_GIT_RAW, body=search_body)
        logger.info(f'{i} removing flag doc with polling: {res}')
        if res['hits']['total']['value'] == 0:
            # or, use res['deleted'] !=0 (or == 1)?
            break


def init_sync_git_datas(git_url, owner, repo, proxy_config, opensearch_conn_datas, git_save_local_path=None):
    # 克隆版本库
    repo_path = f'{git_save_local_path["PATH"]}/{owner}/{repo}'
    if os.path.exists(repo_path):
        shutil.rmtree(repo_path)

    repo_info = Repo.clone_from(url=git_url, to_path=repo_path, config=proxy_config)

    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_datas)
    # 删除在数据库中已经存在的此项目数据
    # TODO Looking for a better solution rather than Create and Destroy a flag doc before/after deleting old data
    # The flag doc here is used to makesure the (owner, repo) at least has ONE place-holder doc event
    # when we try to remove all the real docs.
    # Since the daily-sync DAGs read uniq (owner, repo) pairs from OpenSearch with aggs. The data deletion
    # might make the daily-sync lose some being-deleted (owner, repo) pairs without place holder.
    flag_doc_uuid = insert_flag_doc(opensearch_client, owner, repo)
    delete_old_data(owner=owner, repo=repo, client=opensearch_client)
    remove_flag_doc(opensearch_client, owner, repo, flag_doc_uuid)
    bulk_data_tp = {
        "_index": OPENSEARCH_GIT_RAW,
        "_source": {
            "search_key": {
                "owner": owner, "repo": repo, "origin": git_url, 'updated_at': 0, 'if_sync': 0
            },
            "raw_data": {
                "message": "",
                "hexsha": "",
                "parents": "",
                "author_tz": "",
                "committer_tz": "",
                "author_name": "",
                "author_email": "",
                "committer_name": "",
                "committer_email": "",
                "authored_date": "",
                "authored_timestamp": "",
                "committed_date": "",
                "committed_timestamp": "",
                "files": "",
                "total": "",
                "if_merged": False
            }
        }
    }
    repo_iter_commits = repo_info.iter_commits()
    now_count = 0
    all_git_list = []
    # 实例化opensearchapi
    opensearch_api = OpensearchAPI()
    for commit in repo_iter_commits:
        commit_stats = commit.stats
        files = commit_stats.files
        files_list = []
        for file in files:
            file_dict = files[file]
            file_dict["file_name"] = file
            files_list.append(file_dict)
        bulk_data = copy.deepcopy(bulk_data_tp)
        bulk_data["_source"]["search_key"]["updated_at"] = int(datetime.datetime.now().timestamp() * 1000)
        bulk_data["_source"]["raw_data"]["message"] = commit.message
        bulk_data["_source"]["raw_data"]["hexsha"] = commit.hexsha
        bulk_data["_source"]["raw_data"]["type"] = commit.type
        bulk_data["_source"]["raw_data"]["parents"] = [i.hexsha for i in commit.parents]
        bulk_data["_source"]["raw_data"]["author_tz"] = -int(commit.author_tz_offset / 3600)
        bulk_data["_source"]["raw_data"]["committer_tz"] = -int(commit.committer_tz_offset / 3600)
        bulk_data["_source"]["raw_data"]["author_name"] = commit.author.name
        bulk_data["_source"]["raw_data"]["author_email"] = commit.author.email
        bulk_data["_source"]["raw_data"]["committer_name"] = commit.committer.name
        bulk_data["_source"]["raw_data"]["committer_email"] = commit.committer.email
        bulk_data["_source"]["raw_data"]["authored_date"] = timestamp_to_utc(commit.authored_datetime.timestamp())
        bulk_data["_source"]["raw_data"]["authored_timestamp"] = commit.authored_date
        bulk_data["_source"]["raw_data"]["committed_date"] = timestamp_to_utc(commit.committed_datetime.timestamp())
        bulk_data["_source"]["raw_data"]["committed_timestamp"] = commit.committed_date
        bulk_data["_source"]["raw_data"]["files"] = files_list
        bulk_data["_source"]["raw_data"]["total"] = commit_stats.total
        if_merged = False
        if len(bulk_data["_source"]["raw_data"]["parents"]) == 2:
            if_merged = True
        bulk_data["_source"]["raw_data"]["if_merged"] = if_merged

        now_count = now_count + 1
        all_git_list.append(bulk_data)
        if now_count % 500 == 0:
            success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                                bulk_all_data=all_git_list, owner=owner, repo=repo)
            logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
            logger.info(f"count:{now_count}::{owner}/{repo}::commit.hexsha:{commit.hexsha}")
            all_git_list.clear()

    success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client, bulk_all_data=all_git_list,
                                                        owner=owner, repo=repo)
    logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
    logger.info(f"count:{now_count}::{owner}/{repo}::commit.hexsha:{commit.hexsha}")

    # 这里记录更新位置（gitlog 最上边的一条）
    head_commit = repo_info.head.commit.hexsha
    sync_git_check_update_info(opensearch_client=opensearch_client, owner=owner, repo=repo, head_commit=head_commit)
    return
