from git import Repo
import shutil
import os
# import requests
# import json
# import itertools
from opensearchpy import OpenSearch

from opensearchpy import helpers as OpenSearchHelpers


def init_sync_git_datas(git_url, owner, repo, opensearch_conn_datas, site="github"):
    from airflow.models import Variable
    opensearch_conn_infos = Variable.get("opensearch_conn_data", deserialize_json=True)
    # 克隆版本库
    if os.path.exists('/tmp/{owner}/{repo}'.format(owner=owner, repo=repo)):
        shutil.rmtree('/tmp/{owner}/{repo}'.format(owner=owner, repo=repo))
    repo_info = Repo.clone_from(url=git_url, to_path='/tmp/{owner}/{repo}'.format(owner=owner, repo=repo))

    template = {"_index": "git_raw",
                "_source": {
                            "owner": owner,
                            "repo": repo,
                            "origin": None,
                            "message": None,
                            "hexsha": None,
                            "category": None,
                            "name_rev": None,
                            "parents": None,
                            "author_tz": None,
                            "commiter_tz": None,
                            "author_name": None,
                            "author_email": None,
                            "committer_name": None,
                            "committer_email": None,
                            "authored_date": None,
                            "authored_timestamp": None,
                            "committed_date": None,
                            "committed_timestamp": None,
                            "files": None,
                            "total": None
                            }}
    all_git_list = []
    for commit in repo_info.iter_commits():
        template_copy = template.copy()
        template_copy["_source"]["origin"] = "http://github.com/{owner}/{repo}.git".format(owner=owner, repo=repo)
        template_copy["_source"]["message"] = commit.message
        template_copy["_source"]["hexsha"] = commit.hexsha
        template_copy["_source"]["category"] = commit.type
        template_copy["_source"]["name_rev"] = commit.name_rev
        template_copy["_source"]["parents"] = [i.hexsha for i in commit.parents]
        template_copy["_source"]["author_tz"] = int(commit.author_tz_offset / 3600)
        template_copy["_source"]["commiter_tz"] = int(commit.committer_tz_offset / 3600)
        template_copy["_source"]["author_name"] = commit.author.name
        template_copy["_source"]["author_email"] = commit.author.email
        template_copy["_source"]["authored_date"] = commit.authored_datetime
        template_copy["_source"]["authored_timestamp"] = commit.authored_date
        template_copy["_source"]["committed_date"] = commit.committed_datetime
        template_copy["_source"]["committed_timestamp"] = commit.committed_date
        template_copy["_source"]["committer_name"] = commit.committer.name
        template_copy["_source"]["committer_email"] = commit.committer.email
        template_copy["_source"]["files"] = commit.stats.files
        template_copy["_source"]["total"] = commit.stats.total
        all_git_list.append(template_copy)
    init_sync_bulk_git_datas(all_git_list=all_git_list, opensearch_conn_infos=opensearch_conn_infos)
    return


def init_sync_bulk_git_datas(all_git_list, opensearch_conn_infos):
    client = OpenSearch(
        hosts=[{'host': opensearch_conn_infos["HOST"], 'port': opensearch_conn_infos["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_infos["USER"], opensearch_conn_infos["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )
    success, failed = OpenSearchHelpers.bulk(client=client, actions=all_git_list)
    print("init_sync_bulk_git_datas", success)
    print("init_sync_bulk_git_datas", failed)
    return
