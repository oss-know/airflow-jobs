import copy

import requests
from retrying import retry

from oss_know.libs.util.proxy import GithubTokenProxyAccommodator
from .base import loop_token_proxy, now_timestamp
from .opensearch_api import OpensearchAPI
from ..util.log import logger


class GraphqlQuery:
    github_headers = {'Connection': 'keep-alive', 'Accept-Encoding': 'gzip, deflate, br', 'Accept': '*/*',
                      'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                    'Chrome/96.0.4664.110 Safari/537.36',
                      }

    def __init__(self, owner, repo, accommodator: GithubTokenProxyAccommodator, opensearch_client,
                 opensearch_api: OpensearchAPI, issue_type):
        self.issue_type = issue_type
        self.url = 'https://api.github.com/graphql'
        self.opensearch_api = opensearch_api
        self.opensearch_client = opensearch_client
        self.total_success_send_post_count = 0
        self.owner = owner
        self.repo = repo
        self.session = requests.session()
        self.accommodator = accommodator
        self.issues_tplt = {
            "_index": "graphql_github_issues",
            "_source": {
                "search_key": {
                    "owner": owner, "repo": repo,
                    'issue_type': self.issue_type[:-1],
                    'updated_at': now_timestamp()
                },
                "raw_data": None
            }
        }
        self.issue_timelines_tplt = {
            "_index": "graphql_github_issue_timelines",
            "_source": {
                "search_key": {
                    "owner": owner, "repo": repo,
                    'issue_number': 0,
                    'issue_type': self.issue_type[:-1],
                    'updated_at': now_timestamp()
                },
                "raw_data": None
            }
        }
        self.events_bulk_data = []
        self.issues_bulk_data = []
        self.succeeded_timeline_cursor_bulk_data = []
        self.last_issue_number = 0

    @retry(stop_max_attempt_number=20)
    def post_request(self, query):
        loop_token_proxy(self.session, self.url, self.accommodator)
        logger.info('拉取数据尝试...')
        r = self.session.post(self.url, headers=self.github_headers, json={"query": query})
        response = r.json()
        if response.get('errors') is not None or response.get('message') is not None:
            logger.info(response)
            raise Exception('尝试次数过多,无法获取数据')
        return response

    def extract_issues_fields(self, issue_type, issue_raw_data):
        issue_number = issue_raw_data['number']
        logger.info(f'{issue_type}_number:{issue_number}')
        if issue_raw_data['author']:
            actor_login = issue_raw_data['author']['login']
        else:
            actor_login = ''
        issue_body = issue_raw_data['body']
        created_at = issue_raw_data['createdAt']
        updated_at = issue_raw_data['updatedAt']
        state = issue_raw_data['state']
        closed = issue_raw_data['closed']
        closed_at = issue_raw_data['closedAt']
        if issue_type == 'issues':

            issue_extracted_data = {
                "issue_type": "issue",
                "issue_number": issue_number,
                "actor_login": actor_login,
                "issue_body": issue_body,
                "created_at": created_at,
                "updated_at": updated_at,
                "state": state,
                "closed": closed,
                "closed_at": closed_at,
            }
        else:
            merged = issue_raw_data['merged']
            if issue_raw_data['mergedBy']:
                merged_by_login = issue_raw_data['mergedBy']['login']
            else:
                merged_by_login = ''
            merged_at = issue_raw_data['mergedAt']
            if issue_raw_data['headRef']:
                head_ref_owner_login = issue_raw_data['headRef']['repository']['owner']['login']
                head_ref_repo_name = issue_raw_data['headRef']['repository']['name']
            else:
                head_ref_owner_login = ''
                head_ref_repo_name = ''
            if issue_raw_data['baseRef']:
                base_ref_owner_login = issue_raw_data['baseRef']['repository']['owner']['login']
                base_ref_repo_name = issue_raw_data['baseRef']['repository']['name']
            else:
                base_ref_owner_login = ''
                base_ref_repo_name = ''
            issue_extracted_data = {
                "issue_number": issue_number,
                "actor_login": actor_login,
                "issue_body": issue_body,
                "created_at": created_at,
                "updated_at": updated_at,
                "state": state,
                "closed": closed,
                "closed_at": closed_at,
                "merged": merged,
                "merged_by_login": merged_by_login,
                "merged_at": merged_at,
                "head_ref_owner_login": head_ref_owner_login,
                "head_ref_repo_name": head_ref_repo_name,
                "base_ref_owner_login": base_ref_owner_login,
                "base_ref_repo_name": base_ref_repo_name
            }
        return issue_extracted_data

    def extract_issue_timelines_fields(self, timeline_event_data, events_bulk_data, timeline_tplt, issue_number):

        for event in timeline_event_data:
            timeline_data = copy.deepcopy(timeline_tplt)

            timeline_data['_source']['search_key']['issue_number'] = issue_number

            if event:
                timeline_data['_source']['raw_data'] = event
                events_bulk_data.append(timeline_data)
                if len(events_bulk_data) >= 500:
                    self.opensearch_api.bulk_graphql_github_issues_and_timeline(self.opensearch_client,
                                                                                events_bulk_data,
                                                                                self.owner, self.repo,
                                                                                f"{self.issue_type}: timeline")
                    events_bulk_data.clear()

    def get_issues_and_issue_timelines(self, issues_page_info_end_cursor=''):
        if issues_page_info_end_cursor:
            after_cursor = f', after:"{issues_page_info_end_cursor}"'
        else:
            after_cursor = ''
        if self.issue_type == 'pullRequests':

            query = f"""
                        {{
              repository(name: "{self.repo}", owner: "{self.owner}") {{
                pullRequests(first: 100{after_cursor}) {{
                  pageInfo {{
                    endCursor
                    startCursor
                    hasNextPage
                    hasPreviousPage
                  }}
                  nodes {{
                    id
                    number
                    author {{
                      login
                    }}
                    state
                    merged
                    mergedBy {{
                      login
                    }}
                    mergedAt
                    closed
                    closedAt
                    headRef {{
                      repository {{
                        owner {{
                          login
                        }}
                        name
                      }}
                    }}
                    baseRef {{
                      repository {{
                        owner {{
                          login
                        }}
                        name
                      }}
                    }}
                    body
                    createdAt
                    updatedAt
                    timelineItems(first: 100) {{
                      totalCount
                      pageInfo {{
                        endCursor
                        startCursor
                        hasNextPage
                        hasPreviousPage
                      }}
                      nodes {{
                        __typename
                        ... on HeadRefDeletedEvent {{
                          id
                          actor {{
                            login
                          }}
                          createdAt
                        }}
                        ... on ReferencedEvent{{
                          id
                          actor{{
                            login
                          }}
                          createdAt
                        }}
                        ... on UnassignedEvent {{
                          id
                          actor {{
                            login
                          }}
                          user{{
                            login
                          }}
                        }}
                        ... on MergedEvent {{
                          id
                          actor {{
                            login
                          }}
                          createdAt
                        }}
                        ... on ClosedEvent {{
                          id
                          actor {{
                            login
                          }}
                          createdAt
                        }}
                        ... on AssignedEvent {{
                          id
                          actor {{
                            login
                          }}
                          user {{
                            login
                          }}
                          createdAt
                        }}
                        ... on SubscribedEvent {{
                          id
                          actor {{
                            login
                          }}
                          createdAt
                        }}
                        ... on PullRequestReview {{
                          id
                          author {{
            								login
                          }}        
                          body
                          url
                          state
                          createdAt
                        }}
                        ... on ReviewRequestedEvent {{
                          id
                          actor {{
                            login
                          }}
                          createdAt
                          requestedReviewer {{
            								... on User{{
                              login
                            }}
                          }}
                        }}
                        ... on ReviewRequestRemovedEvent {{
                          id
                          actor {{
                            login

                          }}
                          createdAt
                          requestedReviewer {{
                            ... on User {{
                              login
                            }}
                          }}
                        }}
                        ... on MentionedEvent {{
                          id
                          actor {{
                            login
                          }}
                          createdAt
                        }}
                        ... on LabeledEvent {{
                          id
                          actor {{
                            login
                          }}
                          label {{
                            ... on Label {{
                              name
                            }}
                          }}
                          createdAt
                        }}
                        ... on UnlabeledEvent {{
                          id
                          actor {{
            								login
                          }}
                          label {{
                            ... on Label {{
                              name
                            }}
                          }}
                          createdAt
                        }}
                        ... on CrossReferencedEvent {{
                          id
                          actor {{
            								login
                          }}
                          source{{
                            __typename
                            ... on PullRequest{{
                              number
                            }}
                            ... on Issue{{
                              number
                            }}
                          }}
                          createdAt
                        }}
                        ... on IssueComment {{
                          id
                          author {{
                            login
                          }}
                          body
                          createdAt
                        }}
                        ... on RenamedTitleEvent {{
                          id
                          actor {{
                            login
                          }}
                          createdAt
                        }}
                        ... on HeadRefForcePushedEvent {{
                          id
                          actor {{
                            login
                          }}
                          createdAt
                        }}
                        ... on MilestonedEvent {{
                          id
                          actor {{
                            login
                          }}
                          createdAt
                        }}
                        ... on PullRequestCommit {{
                          id
                          commit {{
                            ... on Commit {{
                              oid
                              author {{
                                user {{
                                  login
                                }}
                              }}
                              committer{{
                                user{{
                                  login
                                }}
                              }}
                              authoredDate
                              committedDate
                            }}
                          }}
                        }}
                      }}
                    }}
                  }}
                }}
              }}
            }}
                        """
        else:
            query = f"""
                        {{
              repository(name: "{self.repo}", owner: "{self.owner}") {{
                issues(first: 100{after_cursor}) {{
                  pageInfo {{
                    endCursor
                    startCursor
                    hasNextPage
                    hasPreviousPage
                  }}
                  nodes {{
                    id
                    number
                    author {{
                      login
                    }}
                    state
                    closed
                    closedAt
                    body
                    createdAt
                    updatedAt
                    timelineItems(first: 100) {{
                      totalCount
                      pageInfo {{
                        endCursor
                        startCursor
                        hasNextPage
                        hasPreviousPage
                      }}
                      nodes{{
                        __typename
                        ... on MilestonedEvent {{
                          id
                          actor {{
                            login
                          }}
                          createdAt
                        }}
                        ... on DemilestonedEvent{{
                          id
                          actor{{
                            login
                          }}
                          createdAt
                        }}
                        ... on UnassignedEvent {{
                          id
                          actor {{
                            login
                          }}
                          user{{
                            login
                          }}
                        }}
                        ... on AssignedEvent {{
                          id
                          actor {{
                            login
                          }}
                          user {{
                            login
                          }}
                          createdAt
                        }}
                        ... on ReopenedEvent{{
                          id
                          actor{{
                            login
                          }}
                          createdAt
                        }}
                        ... on ClosedEvent {{
                          id
                          actor {{
                            login
                          }}
                          createdAt
                        }}
                        ... on MentionedEvent {{
                          id
                          actor {{
                            login
                          }}
                          createdAt
                        }}
                        ... on LabeledEvent {{
                          id
                          actor {{
                            login
                          }}
                          label {{
                            ... on Label {{
                              name
                            }}
                          }}
                          createdAt
                        }}
                        ... on UnlabeledEvent {{
                          id
                          actor {{
            								login
                          }}
                          label {{
                            ... on Label {{
                              name
                            }}
                          }}
                          createdAt
                        }}
                        ... on CrossReferencedEvent {{
                          id
                          actor {{
            								login
                          }}
                          source{{
                            __typename
                            ... on PullRequest{{
                              number
                            }}
                            ... on Issue{{
                              number
                            }}
                          }}
                          createdAt
                        }}
                        ... on IssueComment {{
                          id
                          author {{
                            login
                          }}
                          body
                          createdAt
                        }}
                        ... on ReferencedEvent{{
                          id
                          actor{{
                            login
                          }}
                          createdAt
                        }}
                        ... on SubscribedEvent {{
                          id
                          actor {{
                            login
                          }}
                          createdAt
                        }}
                      }}
                    }}
                  }}
                }}
              }}
            }}
                        """
        try:
            response = self.post_request(query)
        except Exception as e:
            self.save_cache_data()
            if issues_page_info_end_cursor:
                self.save_cursor(issues_page_info_end_cursor, self.issue_type[:-1], self.last_issue_number, 'failed',
                                 self.owner, self.repo)
            else:
                logger.info("第一次请求没有获取成功")
            raise e

        issues = response['data']['repository'][self.issue_type]['nodes']
        issues_page_info = response['data']['repository'][self.issue_type]['pageInfo']
        issues_page_info_has_next_page = issues_page_info['hasNextPage']
        issues_page_info_end_cursor = issues_page_info['endCursor']

        for issue in issues:
            issue_extracted_data = self.extract_issues_fields(self.issue_type, issue)
            issue_data = copy.deepcopy(self.issues_tplt)
            issue_data['_source']['raw_data'] = issue_extracted_data
            self.issues_bulk_data.append(issue_data)
            if len(self.issues_bulk_data) >= 100:
                self.opensearch_api.bulk_graphql_github_issues_and_timeline(self.opensearch_client,
                                                                            self.issues_bulk_data,
                                                                            self.owner, self.repo, self.issue_type)
                self.issues_bulk_data.clear()
            issue_number = issue_extracted_data['issue_number']
            timeline_items = issue['timelineItems']
            issue_timeline_pageinfo_has_next_page = timeline_items['pageInfo']['hasNextPage']
            issue_timeline_pageinfo_end_cursor = timeline_items['pageInfo']['endCursor']
            nodes = timeline_items['nodes']
            self.extract_issue_timelines_fields(nodes, self.events_bulk_data, self.issue_timelines_tplt, issue_number)

            while issue_timeline_pageinfo_has_next_page:
                if self.issue_type == 'issues':
                    next_page_query = f"""
                {{
      repository(name: "{self.repo}", owner: "{self.owner}") {{
        issue(number:{issue_number}) {{	
          timelineItems(first: 100,after:"{issue_timeline_pageinfo_end_cursor}") {{
          totalCount
          pageInfo {{
            endCursor
            startCursor
            hasNextPage
            hasPreviousPage
          }}
          nodes{{
            __typename
            ... on MilestonedEvent {{
              id
              actor {{
                login
              }}
              createdAt
            }}
            ... on DemilestonedEvent{{
              id
              actor{{
                login
              }}
              createdAt
            }}
            ... on UnassignedEvent {{
              id
              actor {{
                login
              }}
              user{{
                login
              }}
            }}
            ... on AssignedEvent {{
              id
              actor {{
                login
              }}
              user {{
                login
              }}
              createdAt
            }}
            ... on ReopenedEvent{{
              id
              actor{{
                login
              }}
              createdAt
            }}
            ... on ClosedEvent {{
              id
              actor {{
                login
              }}
              createdAt
            }}
            ... on MentionedEvent {{
              id
              actor {{
                login
              }}
              createdAt
            }}
            ... on LabeledEvent {{
              id
              actor {{
                login
              }}
              label {{
                ... on Label {{
                  name
                }}
              }}
              createdAt
            }}
            ... on UnlabeledEvent {{
              id
              actor {{
								login
              }}
              label {{
                ... on Label {{
                  name
                }}
              }}
              createdAt
            }}
            ... on CrossReferencedEvent {{
              id
              actor {{
								login
              }}
              source{{
                __typename
                ... on PullRequest{{
                  number
                }}
                ... on Issue{{
                  number
                }}
              }}
              createdAt
            }}
            ... on IssueComment {{
              id
              author {{
                login
              }}
              body
              createdAt
            }}
            ... on ReferencedEvent{{
              id
              actor{{
                login
              }}
              createdAt
            }}
            ... on SubscribedEvent {{
              id
              actor {{
                login
              }}
              createdAt
            }}
          }}
        }}
    }}
  }}
}}
                """
                else:
                    next_page_query = f"""
                {{
  repository(name: "{self.repo}", owner: "{self.owner}") {{
    pullRequest(number:{issue_number}) {{	
      timelineItems(first: 100,after:"{issue_timeline_pageinfo_end_cursor}") {{
          totalCount
          pageInfo {{
            endCursor
            startCursor
            hasNextPage
            hasPreviousPage
          }}
          nodes {{
            __typename
            ... on HeadRefDeletedEvent {{
              id
              actor {{
                login
              }}
              createdAt
            }}
            ... on ReferencedEvent{{
              id
              actor{{
                login
              }}
              createdAt
            }}
            ... on UnassignedEvent {{
              id
              actor {{
                login
              }}
              user{{
                login
              }}
            }}
            ... on MergedEvent {{
              id
              actor {{
                login
              }}
              createdAt
            }}
            ... on ClosedEvent {{
              id
              actor {{
                login
              }}
              createdAt
            }}
            ... on AssignedEvent {{
              id
              actor {{
                login
              }}
              user {{
                login
              }}
              createdAt
            }}
            ... on SubscribedEvent {{
              id
              actor {{
                login
              }}
              createdAt
            }}
            ... on PullRequestReview {{
              id
              author {{
								login
              }}        
              body
              url
              state
              createdAt
            }}
            ... on ReviewRequestedEvent {{
              id
              actor {{
                login
              }}
              createdAt
              requestedReviewer {{
								... on User{{
                  login
                }}
              }}
            }}
            ... on ReviewRequestRemovedEvent {{
              id
              actor {{
                login

              }}
              createdAt
              requestedReviewer {{
                ... on User {{
                  login
                }}
              }}
            }}
            ... on MentionedEvent {{
              id
              actor {{
                login
              }}
              createdAt
            }}
            ... on LabeledEvent {{
              id
              actor {{
                login
              }}
              label {{
                ... on Label {{
                  name
                }}
              }}
              createdAt
            }}
            ... on UnlabeledEvent {{
              id
              actor {{
								login
              }}
              label {{
                ... on Label {{
                  name
                }}
              }}
              createdAt
            }}
            ... on CrossReferencedEvent {{
              id
              actor {{
								login
              }}
              source{{
                __typename
                ... on PullRequest{{
                  number
                }}
                ... on Issue{{
                  number
                }}
              }}
              createdAt
            }}
            ... on IssueComment {{
              id
              author {{
                login
              }}
              body
              createdAt
            }}
            ... on RenamedTitleEvent {{
              id
              actor {{
                login
              }}
              createdAt
            }}
            ... on HeadRefForcePushedEvent {{
              id
              actor {{
                login
              }}
              createdAt
            }}
            ... on MilestonedEvent {{
              id
              actor {{
                login
              }}
              createdAt
            }}
            ... on PullRequestCommit {{
              id
              commit {{
                ... on Commit {{
                  oid
                  author {{
                    user {{
                      login
                    }}
                  }}
                  committer{{
                    user{{
                      login
                    }}
                  }}
                  authoredDate
                  committedDate
                }}
              }}
            }}
          }}
        }}
    }}
  }}
}}
                """
                try:
                    next_page_response = self.post_request(next_page_query)
                except Exception as e:
                    self.save_cursor(issue_timeline_pageinfo_end_cursor, self.issue_type[:-1] + '_timeline',
                                     issue_number, 'failed', self.owner, self.repo)
                    self.save_cache_data()
                    raise e
                next_page_timeline_items = next_page_response['data']['repository'][self.issue_type[:-1]][
                    'timelineItems']
                issue_timeline_pageinfo_has_next_page = next_page_timeline_items['pageInfo']['hasNextPage']
                issue_timeline_pageinfo_end_cursor = next_page_timeline_items['pageInfo']['endCursor']
                nodes = timeline_items['nodes']
                self.extract_issue_timelines_fields(nodes, self.events_bulk_data, self.issue_timelines_tplt,
                                                    issue_number)

            self.last_issue_number = issue_number
            if not issue_timeline_pageinfo_has_next_page:
                self.succeeded_timeline_cursor_bulk_data.append(
                    {
                        "_index": "graphql_cursor",
                        "_source": {"owner": self.owner,
                                    "repo": self.repo,
                                    "update_at": now_timestamp(),
                                    "api_type": self.issue_type[:-1] + '_timeline',
                                    "issue_number": issue_number,
                                    "cursor": issue_timeline_pageinfo_end_cursor,
                                    "log": ''}

                    }
                )
            if len(self.succeeded_timeline_cursor_bulk_data) >= 500:
                self.opensearch_api.bulk_graphql_github_issues_and_timeline(self.opensearch_client,
                                                                            self.succeeded_timeline_cursor_bulk_data,
                                                                            self.owner,
                                                                            self.repo, self.issue_type[
                                                                                       :-1] + '_timeline' + ' cursor')
                self.succeeded_timeline_cursor_bulk_data.clear()

        if issues_page_info_has_next_page:
            self.get_issues_and_issue_timelines(issues_page_info_end_cursor)
        else:
            # 游标需要存储更新使用
            repo_pr_end_cursor = issues_page_info_end_cursor
            self.save_cache_data()

            self.save_cursor(repo_pr_end_cursor, self.issue_type[:-1], self.last_issue_number, 'succeeded', self.owner,
                             self.repo)

            self.opensearch_client.close()

    """
    游标位置保存
    
    timeline_更新增量获取
    """

    def save_cursor(self, cursor, api_type, issue_number, log, owner, repo):
        cursor_structure = {
            "_index": "graphql_cursor",
            "_source": {"owner": owner,
                        "repo": repo,
                        "update_at": now_timestamp(),
                        "api_type": api_type,
                        "issue_number": issue_number,
                        "cursor": cursor,
                        "log": log}

        }
        self.opensearch_api.bulk_graphql_github_issues_and_timeline(self.opensearch_client, [cursor_structure], owner,
                                                                    repo, api_type + ' cursor')

    def save_cache_data(self):
        if self.events_bulk_data:
            self.opensearch_api.bulk_graphql_github_issues_and_timeline(self.opensearch_client,
                                                                        self.events_bulk_data,
                                                                        self.owner, self.repo,
                                                                        f"{self.issue_type}: timeline")
            self.events_bulk_data.clear()
        if self.issues_bulk_data:
            self.opensearch_api.bulk_graphql_github_issues_and_timeline(self.opensearch_client, self.issues_bulk_data,
                                                                        self.owner, self.repo, self.issue_type)
            self.issues_bulk_data.clear()

        if self.succeeded_timeline_cursor_bulk_data:
            self.opensearch_api.bulk_graphql_github_issues_and_timeline(self.opensearch_client,
                                                                        self.succeeded_timeline_cursor_bulk_data,
                                                                        self.owner,
                                                                        self.repo, self.issue_type[
                                                                                   :-1] + '_timeline' + ' cursor')
            self.succeeded_timeline_cursor_bulk_data.clear()

    # TODO: 更新api,出错保留cursor,下一次从出错位置获取数据