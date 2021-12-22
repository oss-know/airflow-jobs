import copy
import time

from git import Repo
import shutil
import os
# import requests
# import json
# import itertools
from opensearchpy import OpenSearch
from ..util.base import get_opensearch_client
from opensearchpy import helpers as OpenSearchHelpers
from airflow.models import Variable

opensearch_conn_infos = Variable.get("opensearch_conn_data", deserialize_json=True)


def delete_pre(owner, repo, client):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {
                        "owner.keyword": {
                            "value": owner
                        }
                    }}, {"term": {
                        "repo.keyword": {
                            "value": repo
                        }
                    }}
                ]
            }
        }
    }
    client.delete_by_query(index="git_raw", body=query)
    print("删除数据成功")


def init_sync_git_datas(git_url, owner, repo, proxy_config, opensearch_conn_datas, site="github"):
    # 克隆版本库
    if os.path.exists('/tmp/{owner}/{repo}'.format(owner=owner, repo=repo)):
        shutil.rmtree('/tmp/{owner}/{repo}'.format(owner=owner, repo=repo))
    # "https.proxy='socks5://127.0.0.1:7890'"
    if proxy_config:
        repo_info = Repo.clone_from(url=git_url, to_path='/tmp/{owner}/{repo}'.format(owner=owner, repo=repo),
                                    config=proxy_config
                                    )
    else:
        repo_info = Repo.clone_from(url=git_url, to_path='/tmp/{owner}/{repo}'.format(owner=owner, repo=repo),
                                    )

    client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)
    all_git_list = []
    # 删除在数据库中已经存在的此项目数据
    delete_pre(owner=owner, repo=repo, client=client)
    # for commit in repo_info.iter_commits():
    #     files = commit.stats.files
    #     files_list = []
    #     for file in files:
    #         file_dict = {}
    #         file_dict["file_name"] = file
    #         file_info = files[file]
    #         for info in file_info:
    #             file_dict[info] = file_info.get(info)
    #         files_list.append(file_dict)
    #     bulk_data = {"_index": "git_raw",
    #                  "_source": {
    #                      "owner": owner,
    #                      "repo": repo,
    #                      "origin": "http://github.com/{owner}/{repo}.git".format(owner=owner, repo=repo),
    #                      "message": commit.message,
    #                      "hexsha": commit.hexsha,
    #                      "category": commit.type,
    #                      "name_rev": commit.name_rev,
    #                      "parents": [i.hexsha for i in commit.parents],
    #                      "author_tz": int(commit.author_tz_offset / 3600),
    #                      "commiter_tz": int(commit.committer_tz_offset / 3600),
    #                      "author_name": commit.author.name,
    #                      "author_email": commit.author.email,
    #                      "committer_name": commit.committer.name,
    #                      "committer_email": commit.committer.email,
    #                      "authored_date": commit.authored_datetime,
    #                      "authored_timestamp": commit.authored_date,
    #                      "committed_date": commit.committed_datetime,
    #                      "committed_timestamp": commit.committed_date,
    #                      "files": files_list,
    #                      "total": commit.stats.total
    #                  }}
    #     all_git_list.append(bulk_data)
    #     # print(all_git_list)
    #     if len(all_git_list) > 1000:
    #         # 批量插入数据
    #         init_sync_bulk_git_datas(all_git_list=all_git_list, client=client)
    #         print("{owner}/{repo}, commit.hexsha:{sha}".format(owner=owner, repo=repo, sha=commit.hexsha))
    #         all_git_list.clear()
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
        bulk_data = copy.deepcopy(bulk_data_te)
        bulk_data["_source"]["row_data"]["message"] = commit.message
        bulk_data["_source"]["row_data"]["hexsha"] = commit.hexsha
        bulk_data["_source"]["row_data"]["type"] = commit.type
        # bulk_data["_source"]["row_data"]["name_rev"] = commit.name_rev
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
            success, failed = init_sync_bulk_git_datas(all_git_list=all_git_list, client=client)
            print("init_sync_bulk_git_datas::success:{success},failed:{failed}".format(success=success, failed=failed))
            print("datatime:{time}::count:{count}::{owner}/{repo}::commit.hexsha:{sha}".format(
                time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                count=now_count,
                owner=owner, repo=repo,
                sha=commit.hexsha))
            all_git_list.clear()

    #                   init_sync_bulk_git_datas(all_git_list=all_git_list, client=client)
    # print("{owner}/{repo}, commit.hexsha:{sha}".format(owner=owner, repo=repo, sha=commit.hexsha))
    # all_git_list.clear()
    success, failed = init_sync_bulk_git_datas(all_git_list=all_git_list, client=client)
    print("init_sync_bulk_git_datas::success:{success},failed:{failed}".format(success=success, failed=failed))
    print("datatime:{time}::count:{count}::{owner}/{repo}::commit.hexsha:{sha}".format(
        time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        count=now_count,
        owner=owner, repo=repo,
        sha=commit.hexsha))
    all_git_list.clear()

    return



def init_sync_bulk_git_datas(all_git_list, client):
    success, failed = OpenSearchHelpers.bulk(client=client, actions=all_git_list)
    return success, failed
