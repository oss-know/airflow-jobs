from opensearchpy import helpers
from opensearchpy import OpenSearch
from loguru import logger
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.base_dict.opensearch_index import (OPENSEARCH_GIT_RAW,
                                          OPENSEARCH_INDEX_GITHUB_COMMITS,
                                          OPENSEARCH_GIT_GITHUB_CLEAN,
                                          OPENSEARCH_INDEX_GITHUB_PROFILE)
from oss_know.libs.util.opensearch_api import OpensearchAPI


def data_clean(owner, repo, opensearch_conn_datas):
    # opensearch_client = OpenSearch(
    #     hosts=[{'host': "192.168.8.102", 'port': 9200}],
    #     http_compress=True,
    #     http_auth=("admin", "admin"),
    #     use_ssl=True,
    #     verify_certs=False,
    #     ssl_assert_hostname=False,
    #     ssl_show_warn=False
    # )
    # 先有opensearch客户端查询数据
    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)
    delete_old_data(owner=owner,
                    repo=repo,
                    client=opensearch_client)
    # 然后从git_raw中查询数据
    git_results = helpers.scan(client=opensearch_client,
                               index=OPENSEARCH_GIT_RAW,
                               query={
                                   "query": {
                                       "bool": {"must": [
                                           {"term": {
                                               "search_key.owner.keyword": {
                                                   "value": owner
                                               }
                                           }},
                                           {"term": {
                                               "search_key.repo.keyword": {
                                                   "value": repo
                                               }
                                           }}
                                       ]}
                                   }
                               },
                               doc_type="_doc"
                               )
    now_count = 0
    all_bulk_data = []
    opensearch_api = OpensearchAPI()
    for commit in git_results:
        git_row_data = commit["_source"]["row_data"]
        hexsha = git_row_data["hexsha"]
        message = git_row_data["message"]
        author_name = git_row_data["author_name"]
        author_email = git_row_data["author_email"]
        committer_name = git_row_data["committer_name"]
        committer_email = git_row_data["committer_email"]
        authored_date = git_row_data["authored_date"]
        authored_timestamp = git_row_data["authored_timestamp"]
        authored_tz = git_row_data["author_tz"]
        committed_date = git_row_data["committed_date"]
        committed_timestamp = git_row_data["committed_timestamp"]
        committed_tz = git_row_data["committer_tz"]
        files = git_row_data["files"]
        total = git_row_data["total"]
        if_merged = False
        if len(git_row_data["parents"]) == 2:
            if_merged = True

        github_commit_results = opensearch_client.search(index=OPENSEARCH_INDEX_GITHUB_COMMITS, body={
            "query": {
                "term": {
                    "raw_data.sha.keyword": {
                        "value": hexsha
                    }
                }
            }
        })
        if len(github_commit_results["hits"]["hits"]) == 0:
            logger.warning(f"owner:{owner}::repo:{repo}::commit_id:{hexsha}:: 在github commit中不存在这个commit_id")
            continue

        author_login = None
        committer_login = None
        author_id = None
        committer_id = None
        raw_data = github_commit_results["hits"]["hits"][0]["_source"]["raw_data"]
        author_profiles = []
        committer_profiles = []

        # 判断是否能和github上的账号对应上
        if raw_data["author"] is not None:
            author_id = raw_data["author"]["id"]
            author_login = raw_data["author"]["login"]
            author_profiles_results = get_profile(opensearch_client=opensearch_client, login=author_login)
            if len(author_profiles_results["hits"]["hits"]) != 0:
                profiles = author_profiles_results["hits"]["hits"]
                for profile in profiles:
                    author_profiles.append(profile["_source"])
        else:
            logger.warning(f"owner:{owner}::repo:{repo}::commit_id:{hexsha}:: author和github上的账号对应不上")

        # 判断是否能和github上的账号对应上
        if raw_data["committer"] is not None:
            committer_login = raw_data["committer"]["login"]
            committer_id = raw_data["committer"]["id"]
            committer_profile_results = get_profile(opensearch_client=opensearch_client, login=committer_login)
            if len(committer_profile_results["hits"]["hits"]) != 0:
                profiles = committer_profile_results["hits"]["hits"]
                for profile in profiles:
                    committer_profiles.append(profile["_source"])
        else:
            logger.warning(f"owner:{owner}::repo:{repo}::commit_id:{hexsha}:: committer和github上的账号对应不上")
        # print(author_profiles, committer_profiles)

        # 数据都查到了设置一个模板然后把数据塞进模板里，然后放入列表批量插入一个列表中
        all_data = {"_index": OPENSEARCH_GIT_GITHUB_CLEAN, "_source": {
            "owner": owner,
            "repo": repo,
            "sha": hexsha,
            "message": message,
            "author_name": author_name,
            "author_email": author_email,
            "committer_name": committer_name,
            "committer_email": committer_email,
            "authored_date": authored_date,
            "authored_timestamp": authored_timestamp,
            "authored_tz": authored_tz,
            "committed_date": committed_date,
            "committed_timestamp": committed_timestamp,
            "committed_tz": committed_tz,
            "total_added": total["insertions"],
            "total_deleted": total["deletions"],
            "total_changed_lines": total["lines"],
            "total_changed_files": total["files"],
            "author_profiles": author_profiles,
            "committer_profiles": committer_profiles,
            "author_login": author_login,
            "committer_login": committer_login,
            "author_id": author_id,
            "committer_id": committer_id,
            "files": files,
            "if_merged": if_merged
        }}
        all_bulk_data.append(all_data)
        now_count += 1
        if now_count % 500 == 0:
            success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                                bulk_all_data=all_bulk_data,
                                                                owner=owner,
                                                                repo=repo)
            logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
            logger.info(f"count:{now_count}::{owner}/{repo}::commit.hexsha:{hexsha}")
            all_bulk_data.clear()
    success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                        bulk_all_data=all_bulk_data,
                                                        owner=owner,
                                                        repo=repo)
    logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
    logger.info(f"count:{now_count}::{owner}/{repo}::commit.hexsha:{hexsha}")


def get_profile(opensearch_client, login):
    user_profile = opensearch_client.search(index=OPENSEARCH_INDEX_GITHUB_PROFILE,
                                            body={
                                                "query": {
                                                    "term": {
                                                        "login.keyword": {
                                                            "value": login
                                                        }
                                                    }
                                                }, "sort": [
                                                    {
                                                        "updated_at": {
                                                            "order": "desc"
                                                        }
                                                    }
                                                ], "_source": ["name", "company", "blog", "location", "email",
                                                               "hireable", "bio",
                                                               "twitter_username"]
                                            })
    return user_profile


def do_opensearch_bulk(opensearch_client, bulk_all_data, owner, repo):
    logger.info(f"owner:{owner},repo:{repo}::do_opensearch_bulk")
    success, failed = helpers.bulk(client=opensearch_client, actions=bulk_all_data)
    return success, failed


def delete_old_data(owner, repo, client):
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
    response = client.delete_by_query(index=OPENSEARCH_GIT_GITHUB_CLEAN, body=query)
    logger.info(response)


