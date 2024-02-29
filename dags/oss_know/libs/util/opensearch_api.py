import copy
import datetime
import json
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple

import dateutil
import opensearchpy
import psycopg2
import requests
import urllib3
from opensearchpy import helpers as opensearch_helpers
from opensearchpy.exceptions import OpenSearchException
from tenacity import *

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_COMMITS, OPENSEARCH_INDEX_GITHUB_ISSUES, \
    OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE, OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS, \
    OPENSEARCH_INDEX_CHECK_SYNC_DATA, OPENSEARCH_INDEX_GITHUB_PROFILE, OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS, \
    OPENSEARCH_GIT_RAW, OPENSEARCH_INDEX_GITHUB_RELEASES
from oss_know.libs.util.airflow import get_postgres_conn
from oss_know.libs.util.base import infer_country_company_geo_insert_into_profile, inferrers, now_timestamp
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.util.log import logger


class OpenSearchAPIException(Exception):
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status


# TODO Is OpenSearchAPI class a good choice to provide the collection of opensearch utils?
#  When the OpenSearchAPI is instantiated somewhere and only part of the utils are need, the unused
#  methods are just wasted. So maybe it's better to put the methods into helper functions in this
#  module and let the caller code decide exactly which one(s) to import and call.
class OpensearchAPI:
    def bulk_github_commits(self, opensearch_client, github_commits, owner, repo, if_sync) -> Tuple[int, int]:
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
                template = {
                    "_index": OPENSEARCH_INDEX_GITHUB_COMMITS,
                    "_source": {
                        "search_key": {
                            "owner": owner, "repo": repo,
                            'updated_at': int(datetime.datetime.now().timestamp() * 1000),
                            'if_sync': if_sync
                        },
                        "raw_data": None
                    }
                }
                commit_item = copy.deepcopy(template)
                commit_item["_source"]["raw_data"] = now_commit
                bulk_all_github_commits.append(commit_item)

        if len(bulk_all_github_commits) > 0:
            success, failed = self.do_opensearch_bulk(opensearch_client, bulk_all_github_commits, owner, repo)
            logger.info(
                f"current github commits page insert count：{len(bulk_all_github_commits)},success:{success},"
                f"failed:{failed}")
            return success, failed
        else:
            return 0, 0

    def bulk_github_issues(self, opensearch_client, github_issues, owner, repo, if_sync):
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

            template = {
                "_index": OPENSEARCH_INDEX_GITHUB_ISSUES,
                "_source": {
                    "search_key": {
                        "owner": owner, "repo": repo,
                        'updated_at': now_timestamp(),
                        'if_sync': if_sync
                    },
                    "raw_data": None
                }
            }
            commit_item = copy.deepcopy(template)
            commit_item["_source"]["raw_data"] = now_issue
            bulk_all_github_issues.append(commit_item)
            logger.info(f"add sync github issues number:{now_issue['number']}")

        success, failed = self.do_opensearch_bulk(opensearch_client, bulk_all_github_issues, owner, repo)
        logger.info(f"now page:{len(bulk_all_github_issues)} sync github issues success:{success} & failed:{failed}")

        return success, failed

    def bulk_github_releases(self, opensearch_client, github_releases, owner, repo):
        template = {
            "_index": OPENSEARCH_INDEX_GITHUB_RELEASES,
            "_source": {
                "search_key": {
                    "owner": owner,
                    "repo": repo,
                    # 'updated_at': now_timestamp(),
                    # 'if_sync': if_sync
                },
                "raw_data": None
            }
        }
        bulk_releases = []
        for rel in github_releases:
            release_item = copy.deepcopy(template)
            release_item["_source"]["raw_data"] = rel
            release_item["_source"]["search_key"]["updated_at"] = now_timestamp()

            bulk_releases.append(release_item)

        success, failed = self.do_opensearch_bulk(opensearch_client, bulk_releases, owner, repo)
        logger.info(f"now page:{len(bulk_releases)} sync github issues success:{success} & failed:{failed}")

        return success, failed

    def bulk_graphql_github_issues_and_timeline(self, opensearch_client, bulk_data, owner, repo, api_type):
        success, failed = self.do_opensearch_bulk(opensearch_client, bulk_data, owner, repo)
        logger.info(f"now page:{len(bulk_data)} sync {api_type} success:{success} & failed:{failed}")

    def bulk_github_languages_tags_releases(self, opensearch_client, datas, owner, repo, api_type):
        bulk_data = []

        def parse_data(data):
            template = {
                "_index": api_type,
                "_source": {
                    "search_key": {
                        "owner": owner, "repo": repo,
                        'updated_at': now_timestamp()
                    },
                    "raw_data": None
                }
            }
            commit_item = copy.deepcopy(template)
            commit_item["_source"]["raw_data"] = data
            bulk_data.append(commit_item)
            logger.info(f"add sync {api_type}")

        if api_type == 'github_languages':
            parse_data(datas)
        else:
            for data in datas:
                parse_data(data)

        success, failed = self.do_opensearch_bulk(opensearch_client, bulk_data, owner, repo)
        logger.info(f"now page:{len(bulk_data)} sync {api_type} success:{success} & failed:{failed}")

    def put_profile_into_opensearch(self, github_ids, token_proxy_accommodator, opensearch_client, if_sync,
                                    if_new_person):
        """Put GitHub user profile into opensearch if it is not in opensearch."""
        # 获取github profile
        batch_size = 100
        num_github_ids = len(github_ids)

        with ThreadPoolExecutor(max_workers=10) as executor:
            task_futures = []
            num_finished = 0
            for index, github_id in enumerate(github_ids):
                time.sleep(round(random.uniform(0.01, 0.1), 2))
                task_futures.append(
                    executor.submit(self.do_init_github_profile, github_id, opensearch_client,
                                    token_proxy_accommodator, if_sync, if_new_person))

            for future in as_completed(task_futures):
                future.result()
                num_finished += 1

                if num_finished % batch_size == 0:
                    logger.info(f'{num_finished} / {num_github_ids} profiles initialized')
        logger.info(f'{num_finished} github profiles finished')
        return "Put GitHub user profile into opensearch if it is not in opensearch"

    def do_init_github_profile(self, github_id, opensearch_client,
                               token_proxy_accommodator, if_sync, if_new_person):
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
            latest_github_profile = github_api.get_latest_github_profile(http_session=session,
                                                                         token_proxy_accommodator=token_proxy_accommodator,
                                                                         user_id=github_id)
            for tup in inferrers:
                key, original_key, infer = tup
                latest_github_profile[key] = None
            infer_country_company_geo_insert_into_profile(latest_github_profile)
            opensearch_client.index(index=OPENSEARCH_INDEX_GITHUB_PROFILE,
                                    body={
                                        "search_key": {
                                            'updated_at': now_timestamp(),
                                            'if_sync': if_sync,
                                            'if_new_person': if_new_person
                                        },
                                        "raw_data": latest_github_profile
                                    },
                                    refresh=True)
            logger.info(f"Put the github {github_id}'s profile into opensearch.")
        else:
            logger.info(f"{github_id}'s profile has already existed.")

    def bulk_github_issues_timeline(self, opensearch_client, issues_timelines, owner, repo, number, if_sync):
        bulk_all_datas = []

        for val in issues_timelines:
            template = {
                "_index": OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE,
                "_source": {
                    "search_key": {
                        "owner": owner, "repo": repo, "number": number, "event": None,
                        'updated_at': int(datetime.datetime.now().timestamp() * 1000),
                        'if_sync': if_sync,
                        'uuid': ''.join(str(uuid.uuid1()).split('-'))
                    },
                    "raw_data": None
                }
            }
            append_item = copy.deepcopy(template)
            append_item["_source"]["raw_data"] = val
            append_item["_source"]["search_key"]["event"] = val["event"]
            bulk_all_datas.append(append_item)
            # logger.info(f"add init sync github issues_timeline number:{number}")

        success, failed = self.do_opensearch_bulk(opensearch_client, bulk_all_datas, owner, repo)
        logger.info(f"now page:{len(bulk_all_datas)} sync github issues_timeline success:{success} & failed:{failed}")

    def bulk_github_issues_comments(self, opensearch_client, issues_comments, owner, repo, number, if_sync):
        bulk_all_github_issues_comments = []

        for val in issues_comments:
            template = {
                "_index": OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS,
                "_source": {
                    "search_key": {
                        "owner": owner, "repo": repo, "number": number,
                        'updated_at': int(datetime.datetime.now().timestamp() * 1000),
                        'if_sync': if_sync
                    },
                    "raw_data": None
                }
            }
            commit_comment_item = copy.deepcopy(template)
            commit_comment_item["_source"]["raw_data"] = val
            bulk_all_github_issues_comments.append(commit_comment_item)
            # logger.info(f"add init sync github issues comments number:{number}")

        success, failed = self.do_opensearch_bulk(opensearch_client, bulk_all_github_issues_comments, owner, repo)
        logger.info(
            f"now page:{len(bulk_all_github_issues_comments)} sync github issues comments success:{success} & "
            f"failed:{failed}")

    def bulk_github_issues_comments_reaction(self,
                                             opensearch_client,
                                             issues_comments_reactions,
                                             owner,
                                             repo,
                                             number,
                                             comment_id):
        bulk_all_github_issues_comments_reactions = []

        for val in issues_comments_reactions:
            template = {
                "_index": "github_issues_comments_reactions",
                "_source": {
                    "search_key": {
                        "owner": owner,
                        "repo": repo,
                        "number": number,
                        "comment_id": comment_id,
                        'updated_at': int(datetime.datetime.now().timestamp() * 1000)

                    },
                    "raw_data": None
                }
            }
            commit_comment_reaction_item = copy.deepcopy(template)
            commit_comment_reaction_item["_source"]["raw_data"] = val
            bulk_all_github_issues_comments_reactions.append(commit_comment_reaction_item)
            # logger.info(f"add init sync github issues comments number:{number}")

        success, failed = self.do_opensearch_bulk(opensearch_client, bulk_all_github_issues_comments_reactions, owner,
                                                  repo)
        logger.info(
            f"now page:{len(bulk_all_github_issues_comments_reactions)} sync github issues comments success:{success} "
            f"& failed:{failed}")

    # 建立 owner/repo github issues 更新基准
    def set_sync_github_issues_check(self, opensearch_client, owner, repo, now_time):
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
    def set_sync_github_pull_requests_check(self, opensearch_client, owner, repo, now_time):
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
                "prs": {
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

    def set_sync_gits_check(self, opensearch_client, owner,
                            repo, check_point_timestamp):
        now_time = datetime.datetime.now()
        check_update_info = {
            "search_key": {
                "type": "gits",
                "update_time": now_time.strftime('%Y-%m-%dT00:00:00Z'),
                "update_timestamp": datetime.datetime.now().timestamp(),
                "owner": owner,
                "repo": repo
            },
            "gits": {
                "type": "gits",
                "owner": owner,
                "repo": repo,
                "commits": {
                    "owner": owner,
                    "repo": repo,
                    "sync_timestamp": check_point_timestamp,
                }
            }
        }
        opensearch_client.index(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA,
                                body=check_update_info,
                                refresh=True)

    def bulk_github_pull_requests(self, github_pull_requests, opensearch_client, owner, repo, if_sync):
        bulk_all_github_pull_requests = []
        batch_size = 200
        batch = []
        total_success = 0
        total_fail = []

        for now_pr in github_pull_requests:
            template = {
                "_index": OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS,
                "_source": {
                    "search_key": {
                        "owner": owner, "repo": repo,
                        'updated_at': int(datetime.datetime.now().timestamp() * 1000),
                        "if_sync": if_sync
                    },
                    "raw_data": None
                }
            }
            pull_requests_item = copy.deepcopy(template)
            pull_requests_item["_source"]["raw_data"] = now_pr
            batch.append(pull_requests_item)
            if len(batch) >= batch_size:
                success, failed = self.do_opensearch_bulk(opensearch_client, batch, owner, repo)
                total_success += success
                total_fail += failed
                logger.info(
                    f"now page:{len(batch)} sync github pull_requests success:{success}/{total_success} & "
                    f"failed:{failed}/{total_fail}")
                batch = []
            logger.debug(f"add init sync github pull_requests number:{now_pr['number']}")

        if batch:
            success, failed = self.do_opensearch_bulk(opensearch_client, batch, owner, repo)
            total_success += success
            total_fail += failed
            logger.info(
                f"now page:{len(batch)} sync github pull_requests success:{success}/{total_success} & "
                f"failed:{failed}/{total_fail}")

        return success, failed

    # -----------------------------------------

    def sync_bulk_github_pull_requests(self, github_pull_requests, opensearch_client, owner, repo, if_sync):
        bulk_all_github_pull_requests = []
        for now_pr in github_pull_requests:
            # 如果对应 pr number存在则先删除
            del_result = opensearch_client.delete_by_query(index=OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS,
                                                           body={
                                                               "track_total_hits": True,
                                                               "query": {
                                                                   "bool": {
                                                                       "must": [
                                                                           {
                                                                               "term": {
                                                                                   "raw_data.number": {
                                                                                       "value": now_pr["number"]
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
            logger.debug(f"DELETE github pr result:{del_result}")
            template = {
                "_index": OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS,
                "_source": {
                    "search_key": {
                        "owner": owner, "repo": repo,
                        'updated_at': int(datetime.datetime.now().timestamp() * 1000),
                        "if_sync": if_sync
                    },
                    "raw_data": None
                }
            }
            pull_requests_item = copy.deepcopy(template)
            pull_requests_item["_source"]["raw_data"] = now_pr
            bulk_all_github_pull_requests.append(pull_requests_item)
            logger.debug(f"add init sync github pull_requests number:{now_pr['number']}")

        # Add batching, instead of inserting all docs at once.
        success, failed = self.do_opensearch_bulk(opensearch_client, bulk_all_github_pull_requests, owner, repo)
        logger.info(
            f"now page:{len(bulk_all_github_pull_requests)} sync github pull_requests success:{success} & failed:"
            f"{failed}")

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
            logger.error(f"retry_state.args:{retry_state.args}")
        except (TypeError) as error:
            logger.error(f"TypeError:{error}")
            logger.error(f"retry_state.args:{retry_state.args}")
        finally:
            if postgres_conn is not None:
                postgres_conn.close()

        return retry_state.outcome.result()

    # retry 防止OpenSearchException
    @retry(stop=stop_after_attempt(3),
           wait=wait_fixed(1),
           retry_error_callback=do_opensearch_bulk_error_callback,
           retry=(retry_if_exception_type(OSError) |
                  retry_if_exception_type(urllib3.exceptions.HTTPError) |
                  retry_if_exception_type(opensearchpy.exceptions.ConnectionTimeout) |
                  retry_if_exception_type(OpenSearchException))
           )
    def do_opensearch_bulk(self, opensearch_client, bulk_all_data, owner, repo):
        logger.debug(f"owner:{owner},repo:{repo}::do_opensearch_bulk")

        success, failed = opensearch_helpers.bulk(client=opensearch_client, actions=bulk_all_data)
        # 强制抛出异常
        # raise OpenSearchException("do_opensearch_bulk Error")
        return success, failed

    def get_uniq_owner_repos(self, opensearch_client, index, excludes=None):
        aggregation_body = {
            "aggs": {
                "uniq_owners": {
                    "terms": {
                        "field": "search_key.owner.keyword",
                        "size": 1000
                    },
                    "aggs": {
                        "uniq_repos": {
                            "terms": {
                                "field": "search_key.repo.keyword",
                                "size": 500
                            }
                        }
                    }
                }
            }
        }

        if index == OPENSEARCH_GIT_RAW:
            aggregation_body['aggs']['uniq_owners']['aggs']['uniq_repos']['aggs'] = {
                "uniq_origin": {
                    "terms": {
                        "field": "search_key.origin.keyword",
                        "size": 10
                    }
                }
            }
        result = opensearch_client.search(index=index, body=aggregation_body)

        uniq_owner_repos = []  # A list of tuple of (owner, repo)
        uniq_owners = result['aggregations']['uniq_owners']['buckets']
        for uniq_owner in uniq_owners:
            owner_name = uniq_owner['key']
            uniq_repos = uniq_owner['uniq_repos']['buckets']
            for uniq_repo in uniq_repos:
                repo_name = uniq_repo['key']
                uniq_item = {
                    'owner': owner_name,
                    'repo': repo_name
                }
                if index == OPENSEARCH_GIT_RAW:
                    uniq_item['origin'] = uniq_repo['uniq_origin']['buckets'][0]['key']

                # The conditions here:
                # 1. excludes is a Falsy value, add (owenr, repo) to the list
                # 2. excludes is not Falsy(the or take effects here), and owner::repo is not in the
                # excludes var, meaning it should not be excluded, add (owner, repo) to the list
                if (not excludes) or f'{owner_name}::{repo_name}' not in excludes:
                    uniq_owner_repos.append(uniq_item)

        return uniq_owner_repos

    def sync_delete(self, opensearch_client, index, search_body, retries=20, interval=0.5):
        opensearch_client.delete_by_query(index, search_body)
        logger.debug(f"Deleting sync with search body: {search_body}")

        for i in range(retries):
            sleep(interval)
            res = opensearch_client.search(index=index, body=search_body)
            if res['hits']['total']['value'] == 0:
                break

    # Get the latest update_timestamp from check_sync_data
    def get_checkpoint(self, opensearch_client, index_type, owner, repo):
        query_body = {
            "size": 1,
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "search_key.type.keyword": {
                                    "value": index_type
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
            },
            "sort": [
                {
                    "search_key.update_timestamp": {
                        "order": "desc"
                    }
                }
            ]
        }

        return opensearch_client.search(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA, body=query_body)
