import git
from git import Repo, exc
import loguru
from loguru import logger
import os
import copy
from opensearchpy import helpers as OpenSearchHelpers

from opensearchpy import OpenSearch
import time
from ..github.init_gits import init_sync_git_datas
from ..util.base import get_opensearch_client, do_opensearch_bulk
OPENSEARCH_LAST_GIT_COMMIT_RECORD = "git_commit_record"
OPENSEARCH_GIT_RAW = "git_raw"


def sync_git_datas(git_url, owner, repo, proxy_config, opensearch_conn_datas, site="github"):
    print("-----------------------------------------------------------------这个方法是sysnc_git_datas")
    repo_path = f'/tmp/{owner}/{repo}'
    git_repo = None
    # 判断有没有这个仓库
    try:
        if os.path.exists(repo_path):
            git_repo = Repo(repo_path)
            # 如果本地已经有之前克隆的项目，执行pull
            git_repo.git.pull()
        else:
            logger.warning("这个文件目录不存在这个项目正在尝试克隆这个项目")
            # 在这个位置调用init
            init_sync_git_datas(git_url=git_url, owner=owner, repo=repo, proxy_config=proxy_config, opensearch_conn_datas=opensearch_conn_datas)
            return
    except exc.InvalidGitRepositoryError as a:
        logger.warning("InvalidGitRepositoryError ：this dir may not a GitRepository")
        logger.warning("这个文件目录不是项目库，正在尝试克隆这个项目")
        # 在这个位置调用init
        init_sync_git_datas(git_url=git_url, owner=owner, repo=repo, proxy_config=proxy_config, opensearch_conn_datas=opensearch_conn_datas)
        return

    # 从数据中获取上次的更新位置

    # 先获取客户端
    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)

    # 去opensearch获取上次的更新点
    last_insert_commit_info = opensearch_client.search(body={
        "size": 1,
        "query": {
            "bool": {"must": [
                {"term": {
                    "owner.keyword": {
                        "value": owner
                    }
                }}, {"term": {
                    "repo.keyword": {
                        "value": repo
                    }
                }}
            ]}
        },
        "sort": [
            {
                "last_insert_timestamp": {
                    "order": "desc"
                }
            }
        ]
    }, index=OPENSEARCH_LAST_GIT_COMMIT_RECORD)
    hits_datas = last_insert_commit_info["hits"]["hits"]

    # git_raw 索引的数据模板
    bulk_data_te = {"_index": "git_raw",
                    "_source": {
                        "search_key": {
                            "owner": owner,
                            "repo": repo,
                            "origin": "http://github.com/{owner}/{repo}.git".format(owner=owner, repo=repo),
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

    if len(hits_datas) == 0:
        print("之前没有记录这个项目")
        # 在这个位置调用init
        init_sync_git_datas(git_url=git_url, owner=owner, repo=repo, proxy_config=proxy_config,
                                      opensearch_conn_datas=opensearch_conn_datas)
        return
    else:
        now_count = 0
        all_git_list = []
        commit_sha = hits_datas[0]["_source"]["commit_sha"]
        commits_iters = git_repo.iter_commits()
        for commit in commits_iters:
            if commit.hexsha == commit_sha:
                break
            files = commit.stats.files
            files_list = []
            for file in files:
                file_dict = {}
                file_dict["file_name"] = file
                file_dict["stats"] = files[file]
                files_list.append(file_dict)
            bulk_data = copy.deepcopy(bulk_data_te)
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
                success, failed = do_opensearch_bulk(opensearch_client=opensearch_client, bulk_all_data=all_git_list, owner=owner, repo=repo)
                print("init_sync_bulk_git_datas::success:{success},failed:{failed}".format(success=success,
                                                                                           failed=failed))
                print("datatime:{time}::count:{count}::{owner}/{repo}::commit.hexsha:{sha}".format(
                    time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                    count=now_count,
                    owner=owner, repo=repo,
                    sha=commit.hexsha))
                all_git_list.clear()
        success, failed = do_opensearch_bulk(opensearch_client=opensearch_client, bulk_all_data=all_git_list, owner=owner, repo=repo)
        print("init_sync_bulk_git_datas::success:{success},failed:{failed}".format(success=success, failed=failed))
        print("datatime:{time}::count:{count}::{owner}/{repo}::commit.hexsha:{sha}".format(
            time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            count=now_count,
            owner=owner, repo=repo,
            sha=commit.hexsha))
        all_git_list.clear()

    # 这里记录更新位置（gitlog 最上边的一条）
    head_commit = git_repo.head.commit
    response = opensearch_client.index(body={
        "owner":owner,
        "repo":repo,
        "commit_sha": head_commit.hexsha,
        "commit_date": head_commit.committed_datetime,
        "last_insert_timestamp": int(time.time())
    }, index=OPENSEARCH_LAST_GIT_COMMIT_RECORD)
    print(response)


# def sync_bulk_git_datas(all_git_list, client):
#     success, failed = OpenSearchHelpers.bulk(client=client, actions=all_git_list)
#     return success, failed

