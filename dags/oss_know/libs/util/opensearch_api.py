import copy
import datetime
import json
from typing import Tuple, Union, List, Any

import dateutil
import psycopg2
import urllib3
import random
from opensearchpy import helpers as opensearch_helpers
from opensearchpy.exceptions import OpenSearchException
from tenacity import *
import requests

from oss_know.libs.util.airflow import get_postgres_conn
from oss_know.libs.util.log import logger
from oss_know.libs.util.github_api import GithubAPI

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_COMMITS, OPENSEARCH_INDEX_GITHUB_ISSUES, \
    OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE, OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS, \
    OPENSEARCH_INDEX_CHECK_SYNC_DATA, OPENSEARCH_INDEX_GITHUB_PROFILE, OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS


class OpenSearchAPIException(Exception):
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status


class OpensearchAPI:
    def bulk_github_commits(self, opensearch_client, github_commits, owner, repo) -> Tuple[int, int]:
        bulk_all_github_commits = []
        for now_commit in github_commits:
            has_commit = opensearch_client.search(index=OPENSEARCH_INDEX_GITHUB_COMMITS,
                                                  body={
                                                      "query": {
                                                          "term": {
                                                              "raw_data.sha.keyword": {
                                                                  "value": now_commit["sha"]
                                                              }
                                                          }
                                                      }
                                                  }
                                                  )
            if len(has_commit["hits"]["hits"]) == 0:
                template = {"_index": OPENSEARCH_INDEX_GITHUB_COMMITS,
                            "_source": {"search_key": {"owner": owner, "repo": repo,
                                                       'updated_at': int(datetime.datetime.now().timestamp() * 1000)},
                                        "raw_data": None}}
                commit_item = copy.deepcopy(template)
                commit_item["_source"]["raw_data"] = now_commit
                bulk_all_github_commits.append(commit_item)

        if len(bulk_all_github_commits) > 0:
            success, failed = self.do_opensearch_bulk(opensearch_client, bulk_all_github_commits, owner, repo)
            logger.info(
                f"current github commits page insert count：{len(bulk_all_github_commits)},success:{success},failed:{failed}")
            return success, failed
        else:
            return 0, 0

    def bulk_github_issues(self, opensearch_client, github_issues, owner, repo):
        bulk_all_github_issues = []

        for now_issue in github_issues:
            # 如果对应 issue number存在则先删除
            del_result = opensearch_client.delete_by_query(index=OPENSEARCH_INDEX_GITHUB_ISSUES,
                                                           body={
                                                               "track_total_hits": True,
                                                               "query": {
                                                                   "bool": {
                                                                       "must": [
                                                                           {
                                                                               "term": {
                                                                                   "raw_data.number": {
                                                                                       "value": now_issue["number"]
                                                                                   }
                                                                               }
                                                                           },
                                                                           {
                                                                               "term": {
                                                                                   "search_key.owner.keyword": {
                                                                                       "value": owner
                                                                                   }
                                                                               }
                                                                           },
                                                                           {
                                                                               "term": {
                                                                                   "search_key.repo.keyword": {
                                                                                       "value": repo
                                                                                   }
                                                                               }
                                                                           }
                                                                       ]
                                                                   }
                                                               }
                                                           })
            logger.info(f"DELETE github issues result:{del_result}")

            template = {"_index": OPENSEARCH_INDEX_GITHUB_ISSUES,
                        "_source": {"search_key": {"owner": owner, "repo": repo,
                                                   'updated_at': int(datetime.datetime.now().timestamp() * 1000)},
                                    "raw_data": None}}
            commit_item = copy.deepcopy(template)
            commit_item["_source"]["raw_data"] = now_issue
            bulk_all_github_issues.append(commit_item)
            logger.info(f"add sync github issues number:{now_issue['number']}")

        success, failed = opensearch_helpers.bulk(client=opensearch_client, actions=bulk_all_github_issues)
        logger.info(f"now page:{len(bulk_all_github_issues)} sync github issues success:{success} & failed:{failed}")

        return success, failed

    def put_profile_into_opensearch(self, github_ids, github_tokens_iter, opensearch_client):
        """Put GitHub user profile into opensearch if it is not in opensearch."""
        # 获取github profile
        for github_id in github_ids:
            logger.info(f'github_profile_user:{github_id}')
            time.sleep(round(random.uniform(0.01, 0.1), 2))
            has_user_profile = opensearch_client.search(index=OPENSEARCH_INDEX_GITHUB_PROFILE,
                                                        body={
                                                            "query": {
                                                                "bool": {
                                                                    "must": [
                                                                        {
                                                                            "term": {
                                                                                "raw_data.id": {
                                                                                    "value": github_id
                                                                                }
                                                                            }
                                                                        }
                                                                    ]
                                                                }
                                                            }
                                                        }
                                                        )
            current_profile_list = has_user_profile["hits"]["hits"]

            if not current_profile_list:
                github_api = GithubAPI()
                session = requests.Session()
                now_github_profile = github_api.get_github_profiles(http_session=session,
                                                                    github_tokens_iter=github_tokens_iter,
                                                                    id_info=github_id)
                opensearch_client.index(index=OPENSEARCH_INDEX_GITHUB_PROFILE,
                                        body={"search_key": {
                                            'updated_at': int(datetime.datetime.now().timestamp()*1000)},
                                            "raw_data": now_github_profile},
                                        refresh=True)
                logger.info(f"Put the github {github_id}'s profile into opensearch.")
            else:
                logger.info(f"{github_id}'s profile has already existed.")
        return "Put GitHub user profile into opensearch if it is not in opensearch"

    def bulk_github_issues_timeline(self, opensearch_client, issues_timelines, owner, repo, number):
        bulk_all_datas = []

        for val in issues_timelines:
            template = {"_index": OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE,
                        "_source": {"search_key": {"owner": owner, "repo": repo, "number": number,
                                                   'updated_at': int(datetime.datetime.now().timestamp() * 1000)},
                                    "raw_data": None}}
            append_item = copy.deepcopy(template)
            append_item["_source"]["raw_data"] = val
            bulk_all_datas.append(append_item)
            logger.info(f"add init sync github issues_timeline number:{number}")

        success, failed = opensearch_helpers.bulk(client=opensearch_client, actions=bulk_all_datas)
        logger.info(f"now page:{len(bulk_all_datas)} sync github issues_timeline success:{success} & failed:{failed}")

    def bulk_github_issues_comments(self, opensearch_client, issues_comments, owner, repo, number):
        bulk_all_github_issues_comments = []

        template = {"_index": OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS,
                    "_source": {"search_key": {"owner": owner, "repo": repo, "number": number,
                                               'updated_at': int(datetime.datetime.now().timestamp() * 1000)},
                                "raw_data": None}}
        commit_comment_item = copy.deepcopy(template)
        commit_comment_item["_source"]["raw_data"] = issues_comments
        bulk_all_github_issues_comments.append(commit_comment_item)
        logger.info(f"add init sync github issues comments number:{number}")

        success, failed = opensearch_helpers.bulk(client=opensearch_client, actions=bulk_all_github_issues_comments)
        logger.info(
            f"now page:{len(bulk_all_github_issues_comments)} sync github issues comments success:{success} & failed:{failed}")

    # 建立 owner/repo github issues 更新基准
    def set_sync_github_issues_check(self, opensearch_client, owner, repo):
        now_time = datetime.datetime.now()
        check_update_info = {
            "search_key": {
                "type": "github_issues",
                "update_time": now_time.strftime('%Y-%m-%dT00:00:00Z'),
                "update_timestamp": now_time.timestamp(),
                "owner": owner,
                "repo": repo
            },
            "github": {
                "type": "github_issues",
                "owner": owner,
                "repo": repo,
                "issues": {
                    "owner": owner,
                    "repo": repo,
                    "sync_datetime": now_time.strftime('%Y-%m-%dT00:00:00Z'),
                    "sync_timestamp": now_time.timestamp()
                }
            }
        }

        opensearch_client.index(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA,
                                body=check_update_info,
                                refresh=True)

    # 建立 owner/repo github pull_requests 更新基准
    def set_sync_github_pull_requests_check(self, opensearch_client, owner, repo):
        now_time = datetime.datetime.now()
        check_update_info = {
            "search_key": {
                "type": "github_pull_requests",
                "update_time": now_time.strftime('%Y-%m-%dT00:00:00Z'),
                "update_timestamp": now_time.timestamp(),
                "owner": owner,
                "repo": repo
            },
            "github": {
                "type": "github_pull_requests",
                "owner": owner,
                "repo": repo,
                "issues": {
                    "owner": owner,
                    "repo": repo,
                    "sync_datetime": now_time.strftime('%Y-%m-%dT00:00:00Z'),
                    "sync_timestamp": now_time.timestamp()
                }
            }
        }

        # 创建 github_pull_requests 的 check 更新记录
        opensearch_client.index(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA,
                                body=check_update_info,
                                refresh=True)

    # 建立 owner/repo github commits 更新基准
    def set_sync_github_commits_check(self, opensearch_client, owner,
                                      repo, since, until):
        now_time = datetime.datetime.now()
        check_update_info = {
            "search_key": {
                "type": "github_commits",
                "update_time": now_time.strftime('%Y-%m-%dT00:00:00Z'),
                "update_timestamp": now_time.timestamp(),
                "owner": owner,
                "repo": repo
            },
            "github": {
                "type": "github_commits",
                "owner": owner,
                "repo": repo,
                "commits": {
                    "owner": owner,
                    "repo": repo,
                    "sync_timestamp": now_time.timestamp(),
                    "sync_since_timestamp": dateutil.parser.parse(since).timestamp(),
                    "sync_until_timestamp": dateutil.parser.parse(until).timestamp(),
                    "sync_since_datetime": since,
                    "sync_until_datetime": until
                }
            }
        }
        opensearch_client.index(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA,
                                body=check_update_info,
                                refresh=True)

    # 建立 github profile更新基准
    def set_sync_github_profiles_check(self, opensearch_client, login, id):
        now_time = datetime.datetime.now()
        check_update_info = {
            "search_key": {
                "type": "github_profiles",
                "update_time": now_time.strftime('%Y-%m-%dT00:00:00Z'),
                "update_timestamp": now_time.timestamp(),
                "login": login,
                "id": id,
            },
            "github": {
                "type": "github_profiles",
                "login": login,
                "id": id,
                "profiles": {
                    "login": login,
                    "id": id,
                    "sync_datetime": now_time.strftime('%Y-%m-%dT00:00:00Z'),
                    "sync_timestamp": now_time.timestamp()
                }
            }
        }

        opensearch_client.index(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA,
                                body=check_update_info,
                                refresh=True)

    def bulk_github_pull_requests(self, github_pull_requests, opensearch_client, owner, repo):
        bulk_all_github_pull_requests = []
        for now_pr in github_pull_requests:
            template = {"_index": OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS,
                        "_source": {"search_key": {"owner": owner, "repo": repo,
                                                   'updated_at': int(datetime.datetime.now().timestamp() * 1000)},
                                    "raw_data": None}}
            pull_requests_item = copy.deepcopy(template)
            pull_requests_item["_source"]["raw_data"] = now_pr
            bulk_all_github_pull_requests.append(pull_requests_item)
            logger.info(f"add init sync github pull_requests number:{now_pr['number']}")

        success, failed = opensearch_helpers.bulk(client=opensearch_client, actions=bulk_all_github_pull_requests)
        logger.info(
            f"now page:{len(bulk_all_github_pull_requests)} sync github pull_requests success:{success} & failed:{failed}")

        return success, failed

    # -----------------------------------------

    def do_opensearch_bulk_error_callback(retry_state):
        postgres_conn = get_postgres_conn()
        sql = '''INSERT INTO retry_data(
                    owner, repo, type, data) 
                    VALUES (%s, %s, %s, %s);'''
        try:
            cur = postgres_conn.cursor()
            owner = retry_state.args[2]
            repo = retry_state.args[3]
            for bulk_item in retry_state.args[1]:
                cur.execute(sql, (owner, repo, 'opensearch_bulk', json.dumps(bulk_item)))
            postgres_conn.commit()
            cur.close()
        except (psycopg2.DatabaseError) as error:
            logger.error(f"psycopg2.DatabaseError:{error}")

        finally:
            if postgres_conn is not None:
                postgres_conn.close()

        return retry_state.outcome.result()

    # retry 防止OpenSearchException
    @retry(stop=stop_after_attempt(3),
           wait=wait_fixed(1),
           retry_error_callback=do_opensearch_bulk_error_callback,
           retry=(retry_if_exception_type(OSError) | retry_if_exception_type(
               urllib3.exceptions.HTTPError) | retry_if_exception_type(OpenSearchException))
           )
    def do_opensearch_bulk(self, opensearch_client, bulk_all_data, owner, repo):
        logger.info(f"owner:{owner},repo:{repo}::do_opensearch_bulk")

        success, failed = opensearch_helpers.bulk(client=opensearch_client, actions=bulk_all_data)
        # 强制抛出异常
        # raise OpenSearchException("do_opensearch_bulk Error")
        return success, failed
