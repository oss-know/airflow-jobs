import os
import copy
import time
import datetime
from git import Repo, exc
from loguru import logger
from oss_know.libs.github.init_gits import init_sync_git_datas
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_CHECK_SYNC_DATA, OPENSEARCH_GIT_RAW
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
            "update_time": now_time.strftime('%Y-%m-%dT00:00:00Z'),
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
                "sync_commit_sha": head_commit,
            }
        }
    }
    response = opensearch_client.index(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA, body=check_update_info)
    logger.info(response)


def sync_git_datas(git_url, owner, repo, proxy_config, opensearch_conn_datas, git_save_local_path=None):
    repo_path = f'{git_save_local_path["PATH"]}/{owner}/{repo}'
    git_repo = None
    # 判断有没有这个仓库
    try:
        if os.path.exists(repo_path):
            git_repo = Repo(repo_path)
            # 如果本地已经有之前克隆的项目，执行pull
            git_repo.git.pull()
        else:
            logger.warning("This project does not exist in this file directory. Attempting to clone this project")
            # 在这个位置调用init
            init_sync_git_datas(git_url=git_url,
                                owner=owner,
                                repo=repo,
                                proxy_config=proxy_config,
                                opensearch_conn_datas=opensearch_conn_datas,
                                git_save_local_path=git_save_local_path)
            return
    except exc.InvalidGitRepositoryError as a:
        logger.error("InvalidGitRepositoryError ：This directory may not a GitRepository")
        return

    # 从数据中获取上次的更新位置

    # 先获取客户端
    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)

    # 去opensearch获取上次的更新点
    last_insert_commit_info = opensearch_client.search(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA,
                                                       body={
                                                           "size": 1,
                                                           "query": {
                                                               "bool": {"must": [
                                                                   {"term": {
                                                                       "search_key.owner.keyword": {
                                                                           "value": owner
                                                                       }
                                                                   }}, {"term": {
                                                                       "search_key.repo.keyword": {
                                                                           "value": repo
                                                                       }
                                                                   }}
                                                               ]}
                                                           },
                                                           "sort": [
                                                               {
                                                                   "search_key.update_timestamp": {
                                                                       "order": "desc"
                                                                   }
                                                               }
                                                           ]
                                                       })
    hits_datas = last_insert_commit_info["hits"]["hits"]

    # git_raw 索引的数据模板
    bulk_data_tp = {"_index": OPENSEARCH_GIT_RAW,
                    "_source": {
                        "search_key": {
                            "owner": owner,
                            "repo": repo,
                            "origin": f"http://github.com/{owner}/{repo}.git",
                            'updated_at': int(datetime.datetime.now().timestamp() * 1000)
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
                    }}

    if len(hits_datas) == 0:
        logger.warning("This item has not been recorded before")
        # 如果在更新记录的索引中没有看到这个项目那么就重新克隆
        init_sync_git_datas(git_url=git_url,
                            owner=owner,
                            repo=repo,
                            proxy_config=proxy_config,
                            opensearch_conn_datas=opensearch_conn_datas,
                            git_save_local_path=git_save_local_path)
        return
    else:
        now_count = 0
        all_git_list = []
        commit_sha = hits_datas[0]["_source"]["git"]["commits"]["sync_commit_sha"]
        commits_iters = git_repo.iter_commits()
        opensearch_api = OpensearchAPI()
        for commit in commits_iters:
            if commit.hexsha == commit_sha:
                break
            files = commit.stats.files
            files_list = []
            for file in files:
                # file_dict = {}
                # file_dict["file_name"] = file
                # file_dict["stats"] = files[file]
                file_dict = files[file]
                file_dict["file_name"] = file
                files_list.append(file_dict)
            bulk_data = copy.deepcopy(bulk_data_tp)
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
            bulk_data["_source"]["raw_data"]["total"] = commit.stats.total
            if_merged = False
            if len(bulk_data["_source"]["raw_data"]["parents"]) == 2:
                if_merged = True
            bulk_data["_source"]["raw_data"]["if_merged"] = if_merged

            now_count = now_count + 1
            all_git_list.append(bulk_data)
            if now_count % 500 == 0:
                success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                                    bulk_all_data=all_git_list,
                                                                    owner=owner,
                                                                    repo=repo)
                logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
                logger.info(f"count:{now_count}::{owner}/{repo}::commit.hexsha:{commit.hexsha}")
                all_git_list.clear()
        success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                            bulk_all_data=all_git_list,
                                                            owner=owner,
                                                            repo=repo)
        logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
        logger.info(f"count:{now_count}::{owner}/{repo}::commit.hexsha:{commit.hexsha}")
        all_git_list.clear()

    # 这里记录更新位置（gitlog 最上边的一条）
    head_commit = git_repo.head.commit.hexsha
    sync_git_check_update_info(opensearch_client=opensearch_client,
                               owner=owner,
                               repo=repo,
                               head_commit=head_commit)
    return
