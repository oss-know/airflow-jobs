from . import init_profile_commen
import itertools
from opensearchpy.helpers import scan as os_scan
import time

OPEN_SEARCH_GITHUB_PROFILE_INDEX = "github_profile"


def load_github_profile_issues_timeline(github_tokens, opensearch_conn_infos, owner, repo):
    github_tokens_iter = itertools.cycle(github_tokens)

    opensearch_client = init_profile_commen.get_opensearch_client(opensearch_conn_infos)

    # 查询owner+repo所有github issues记录用来提取github issue的user
    res = os_scan(client=opensearch_client, index='github_issues_timeline',
                  query={
                      "track_total_hits": True,
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
                      },
                      "size": 10
                  }, doc_type='_doc', timeout='10m')
    all_issues_timeline_users = set([])

    for issue_timeline in res:
        print("========================20211224test=======================")

        issue_timeline_user_login = issue_timeline["_source"]["raw_data"]["user"]["login"]
        all_issues_timeline_users.add(issue_timeline_user_login)

    # 获取github profile
    for issue_timeline_user in all_issues_timeline_users:
        print("该repository中的issue_timeline用户有", issue_timeline_user)
        time.sleep(1)

        has_user_profile = opensearch_client.search(index=OPEN_SEARCH_GITHUB_PROFILE_INDEX,
                                                    body={
                                                        "query": {
                                                            "term": {
                                                                "login.keyword": {
                                                                    "value": issue_timeline_user
                                                                }
                                                            }
                                                        }
                                                    }
                                                    )

        current_profile_list = has_user_profile["hits"]["hits"]

        now_github_profile = init_profile_commen.get_github_profile(github_tokens_iter, issue_timeline_user,
                                                                    opensearch_conn_infos)

        if len(current_profile_list) == 0:
            opensearch_client.index(index=OPEN_SEARCH_GITHUB_PROFILE_INDEX,
                                    body=now_github_profile,
                                    refresh=True)
            print("没有该用户信息，添加该用户成功")
        else:
            print("os中已有该用户信息")
    return "End::load_github_profile_issues_timeline"
