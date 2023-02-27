import datetime
import os

from git import Repo
from loguru import logger

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_GIT_RAW
from oss_know.libs.github.init_gits import init_sync_git_datas
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.opensearch_api import OpensearchAPI


def timestamp_to_utc(timestamp):
    # 10位时间戳
    return datetime.datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%dT%H:%M:%SZ")


def sync_gits_opensearch(git_url, owner, repo, proxy_config, opensearch_conn_datas, git_save_local_path=None):
    repo_path = f'{git_save_local_path["PATH"]}/{owner}/{repo}'
    check_point_timestamp = int(datetime.datetime.now().timestamp() * 1000)

    git_repo = None
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_datas)
    os_repo_commits = opensearch_client.search(index='gits', body=latest_commit_query_body(owner, repo))

    if not os_repo_commits['hits']['hits']:
        logger.warning(f"{owner}/{repo} does not exist in local storage, init it from scratch")
        init_sync_git_datas(git_url=git_url,
                            owner=owner,
                            repo=repo,
                            proxy_config=proxy_config,
                            opensearch_conn_datas=opensearch_conn_datas,
                            git_save_local_path=git_save_local_path)
        return

    if os.path.exists(repo_path):
        git_repo = Repo(repo_path)
        git_repo.remote('origin').fetch()
    else:
        git_repo = Repo.clone_from(url=git_url, to_path=repo_path, config=proxy_config)

    head_in_os = os_repo_commits['hits']['hits'][0]['_source']['raw_data']['hexsha']

    try:
        git_repo.commit(head_in_os)
    except ValueError as e:
        # TODO We actually can walk through the head_in_os's ancestors to look for the merge base
        # but this brings much more complexity(handle multiple parents manually) and search cost

        # So after doing the re-init, we should reset --hard to the remotes/origin/{active_branch}, making
        # it more likely to find the merge base for next sync
        logger.warning(
            f"head in OpenSearch {head_in_os} may not exist in git commit tree, error: {e}, init repo from scratch")
        init_sync_git_datas(git_url=git_url,
                            owner=owner,
                            repo=repo,
                            proxy_config=proxy_config,
                            opensearch_conn_datas=opensearch_conn_datas,
                            git_save_local_path=git_save_local_path)
        return

    active_branch_name = git_repo.active_branch.name
    head_in_repo = git_repo.commit(f'origin/{active_branch_name}').hexsha

    merge_bases = git_repo.merge_base(head_in_os, head_in_repo)
    if not merge_bases:
        logger.warning(f"No merge base found for {owner}/{repo}, init it from scratch")
        init_sync_git_datas(git_url=git_url,
                            owner=owner,
                            repo=repo,
                            proxy_config=proxy_config,
                            opensearch_conn_datas=opensearch_conn_datas,
                            git_save_local_path=git_save_local_path)
        return

    # Get the children after MERGE BASE in two code bases:
    merge_base = merge_bases[0]
    children_in_os = git_repo.iter_commits(f'{merge_base}...{head_in_os}')
    children_in_repo = git_repo.iter_commits(f'{merge_base}...{head_in_repo}')
    logger.info(f'children in opensearch: {merge_base}...{head_in_os}')
    logger.info(f'children in git repo: {merge_base}...{head_in_repo}')

    batch = []
    opensearch_api = OpensearchAPI()

    for c in children_in_os:
        opensearch_client.delete_by_query(index=OPENSEARCH_GIT_RAW, body={
            "query": {
                "match": {
                    "raw_data.hexsha.keyword": c.hexsha
                }
            }
        })

    for c in children_in_repo:
        commit_doc = create_commit_doc(owner, repo, git_repo, c, check_point_timestamp)
        batch.append(commit_doc)

        if len(batch) >= 500:
            success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                                bulk_all_data=batch,
                                                                owner=owner,
                                                                repo=repo)
            logger.debug(f"sync_bulk_git_datas::success:{success}, failed:{failed}")
            batch = []
    # Handle the remaining docs if there are
    if batch:
        success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                            bulk_all_data=batch,
                                                            owner=owner,
                                                            repo=repo)
        logger.debug(f"sync_bulk_git_datas::success:{success}, failed:{failed}")

    # 加入check_point点
    opensearch_api.set_sync_gits_check(opensearch_client=opensearch_client,
                                       owner=owner,
                                       repo=repo,
                                       check_point_timestamp=check_point_timestamp)

    # Keep track of the current latest remote, making it more likely to get the merge base for next sync
    git_repo.git.reset('--hard', f'origin/{active_branch_name}')


def create_commit_doc(owner, repo, git_repo, sha, check_point_timestamp):
    commit_doc = {"_index": OPENSEARCH_GIT_RAW,
                  "_source": {
                      "search_key": {
                          "owner": owner,
                          "repo": repo,
                          "origin": f"https://github.com/{owner}/{repo}.git",
                          'updated_at': 0,
                          'if_sync': 1
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
    commit = git_repo.commit(sha)
    files = commit.stats.files
    files_list = []
    for file in files:
        file_dict = files[file]
        file_dict["file_name"] = file
        files_list.append(file_dict)
    commit_doc["_source"]["search_key"]["updated_at"] = check_point_timestamp
    commit_doc["_source"]["raw_data"]["message"] = commit.message
    commit_doc["_source"]["raw_data"]["hexsha"] = commit.hexsha
    commit_doc["_source"]["raw_data"]["type"] = commit.type
    commit_doc["_source"]["raw_data"]["parents"] = [i.hexsha for i in commit.parents]
    commit_doc["_source"]["raw_data"]["author_tz"] = -int(commit.author_tz_offset / 3600)
    commit_doc["_source"]["raw_data"]["committer_tz"] = -int(commit.committer_tz_offset / 3600)
    commit_doc["_source"]["raw_data"]["author_name"] = commit.author.name
    commit_doc["_source"]["raw_data"]["author_email"] = commit.author.email
    commit_doc["_source"]["raw_data"]["committer_name"] = commit.committer.name
    commit_doc["_source"]["raw_data"]["committer_email"] = commit.committer.email
    commit_doc["_source"]["raw_data"]["authored_date"] = timestamp_to_utc(commit.authored_datetime.timestamp())
    commit_doc["_source"]["raw_data"]["authored_timestamp"] = commit.authored_date
    commit_doc["_source"]["raw_data"]["committed_date"] = timestamp_to_utc(commit.committed_datetime.timestamp())
    commit_doc["_source"]["raw_data"]["committed_timestamp"] = commit.committed_date
    commit_doc["_source"]["raw_data"]["files"] = files_list
    commit_doc["_source"]["raw_data"]["total"] = commit.stats.total
    commit_doc["_source"]["raw_data"]["if_merged"] = len(commit_doc["_source"]["raw_data"]["parents"]) > 0
    return commit_doc


def latest_commit_query_body(owner, repo):
    query_body = owner_repo_query_body(owner, repo)
    query_body['sort'] = [
        {
            "raw_data.committed_date": {
                "order": "desc"
            }
        }
    ]
    query_body['size'] = 1

    return query_body


def owner_repo_query_body(owner, repo):
    return {
        "query": {
            "bool": {
                "must": [
                    {"term": {
                        "search_key.owner.keyword": {
                            "value": owner
                        }
                    }
                    },
                    {"term": {
                        "search_key.repo.keyword": {
                            "value": repo
                        }
                    }
                    }
                ]
            }
        }
    }
