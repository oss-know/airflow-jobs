from git import Repo
import shutil
import os
# import requests
# import json
# import itertools
from opensearchpy import OpenSearch
from ..util.base import get_client
from opensearchpy import helpers as OpenSearchHelpers

client = get_client()


def init_sync_git_datas(git_url, owner, repo, opensearch_conn_datas, site="github"):

    # 克隆版本库
    if os.path.exists('/tmp/{owner}/{repo}'.format(owner=owner, repo=repo)):
        shutil.rmtree('/tmp/{owner}/{repo}'.format(owner=owner, repo=repo))
    repo_info = Repo.clone_from(url=git_url, to_path='/tmp/{owner}/{repo}'.format(owner=owner, repo=repo))

    all_git_list = []
    for commit in repo_info.iter_commits():
        files = commit.stats.files
        files_list = []
        for file in files:
            file_dict = {}
            file_dict["file_name"] = file
            file_info = files[file]
            for info in file_info:
                file_dict[info] = file_info.get(info)
            files_list.append(file_dict)
        bulk_data = {"_index": "git_raw",
                    "_source": {
                        "owner": owner,
                        "repo": repo,
                        "origin": "http://github.com/{owner}/{repo}.git".format(owner=owner, repo=repo),
                        "message": commit.message,
                        "hexsha": commit.hexsha,
                        "category": commit.type,
                        "name_rev": commit.name_rev,
                        "parents": [i.hexsha for i in commit.parents],
                        "author_tz": int(commit.author_tz_offset / 3600),
                        "commiter_tz": int(commit.committer_tz_offset / 3600),
                        "author_name": commit.author.name,
                        "author_email": commit.author.email,
                        "committer_name": commit.committer.name,
                        "committer_email": commit.committer.email,
                        "authored_date": commit.authored_datetime,
                        "authored_timestamp": commit.authored_date,
                        "committed_date": commit.committed_datetime,
                        "committed_timestamp": commit.committed_date,
                        "files": files_list,
                        "total": commit.stats.total
                    }}
        all_git_list.append(bulk_data)
    print(all_git_list)
    init_sync_bulk_git_datas(all_git_list=all_git_list)
    return


def init_sync_bulk_git_datas(all_git_list):
    success, failed = OpenSearchHelpers.bulk(client=client, actions=all_git_list)
    print("init_sync_bulk_git_datas", success)
    print("init_sync_bulk_git_datas", failed)
    return
