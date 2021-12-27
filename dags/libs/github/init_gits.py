import copy
import time
import shutil
import os
from loguru import logger
from git import Repo
from airflow.models import Variable

from ..util.base import get_opensearch_client, do_opensearch_bulk, sync_git_check_update_info


def delete_pre(owner, repo, client):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {
                        "search_key.owner.keyword": {
                            "value": owner
                        }
                    }}, {"term": {
                        "search_key.repo.keyword": {
                            "value": repo
                        }
                    }}
                ]
            }
        }
    }
    response = client.delete_by_query(index="git_raw", body=query)
    logger.info(response)


def init_sync_git_datas(git_url, owner, repo, proxy_config, opensearch_conn_datas, git_save_local_path=None):
    # 克隆版本库
    repo_path = f'{git_save_local_path["PATH"]}/{owner}/{repo}'
    if os.path.exists(repo_path):
        shutil.rmtree(repo_path)

    if proxy_config:
        repo_info = Repo.clone_from(url=git_url, to_path=repo_path,
                                    config=proxy_config
                                    )
    else:
        repo_info = Repo.clone_from(url=git_url, to_path=repo_path,
                                    )

    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)
    all_git_list = []
    # 删除在数据库中已经存在的此项目数据
    delete_pre(owner=owner, repo=repo, client=opensearch_client)

    bulk_data_tp = {"_index": "git_raw",
                    "_source": {
                        "search_key": {
                            "owner": owner,
                            "repo": repo,
                            "origin": f"http://github.com/{owner}/{repo}.git",
                        },
                        "row_data": {
                            "message": "",
                            "hexsha": "",
                            "category": "",
                            "name_rev": "",
                            "parents": "",
                            "author_tz": "",
                            "commiter_tz": "",
                            "author_name": "",
                            "author_email": "",
                            "committer_name": "",
                            "committer_email": "",
                            "authored_date": "",
                            "authored_timestamp": "",
                            "committed_date": "",
                            "committed_timestamp": "",
                            "files": "",
                            "total": ""
                        }
                    }}
    repo_iter_commits = repo_info.iter_commits()
    now_count = 0
    all_git_list = []
    for commit in repo_iter_commits:
        files = commit.stats.files
        files_list = []
        for file in files:
            file_dict = {}
            file_dict["file_name"] = file
            file_dict["stats"] = files[file]
            files_list.append(file_dict)
        bulk_data = copy.deepcopy(bulk_data_tp)
        bulk_data["_source"]["row_data"]["message"] = commit.message
        bulk_data["_source"]["row_data"]["hexsha"] = commit.hexsha
        bulk_data["_source"]["row_data"]["type"] = commit.type
        bulk_data["_source"]["row_data"]["parents"] = [i.hexsha for i in commit.parents]
        bulk_data["_source"]["row_data"]["author_tz"] = int(commit.author_tz_offset / 3600)
        bulk_data["_source"]["row_data"]["commiter_tz"] = int(commit.committer_tz_offset / 3600)
        bulk_data["_source"]["row_data"]["author_name"] = commit.author.name
        bulk_data["_source"]["row_data"]["author_email"] = commit.author.email
        bulk_data["_source"]["row_data"]["committer_name"] = commit.committer.name
        bulk_data["_source"]["row_data"]["committer_email"] = commit.committer.email
        bulk_data["_source"]["row_data"]["authored_date"] = commit.authored_datetime
        bulk_data["_source"]["row_data"]["authored_timestamp"] = commit.authored_date
        bulk_data["_source"]["row_data"]["committed_date"] = commit.committed_datetime
        bulk_data["_source"]["row_data"]["committed_timestamp"] = commit.committed_date
        bulk_data["_source"]["row_data"]["files"] = files_list
        bulk_data["_source"]["row_data"]["total"] = commit.stats.total

        now_count = now_count + 1
        all_git_list.append(bulk_data)
        if now_count % 500 == 0:
            success, failed = do_opensearch_bulk(opensearch_client=opensearch_client,
                                                 bulk_all_data=all_git_list,
                                                 owner=owner,
                                                 repo=repo)
            logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
            logger.info(f"count:{now_count}::{owner}/{repo}::commit.hexsha:{commit.hexsha}")
            all_git_list.clear()


    success, failed = do_opensearch_bulk(opensearch_client=opensearch_client,
                                         bulk_all_data=all_git_list,
                                         owner=owner,
                                         repo=repo)
    logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
    logger.info(f"count:{now_count}::{owner}/{repo}::commit.hexsha:{commit.hexsha}")
    all_git_list.clear()

    # 这里记录更新位置（gitlog 最上边的一条）
    head_commit = repo_info.head.commit.hexsha
    sync_git_check_update_info(opensearch_client=opensearch_client,
                               owner=owner,
                               repo=repo,
                               head_commit=head_commit)
    return


