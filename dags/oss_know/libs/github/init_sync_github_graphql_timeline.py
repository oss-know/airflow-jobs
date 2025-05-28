# -*-coding:utf-8-*-
from oss_know.libs.util.GithubGraphql import GraphqlQuery
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.opensearch_api import OpensearchAPI


def init_graphql_pr_timeline(opensearch_conn_infos, owner, repo, token_proxy_accommodator,issue_type):
    opensearch_client = get_opensearch_client(opensearch_conn_infos)
    opensearch_api = OpensearchAPI()
    graphql_query = GraphqlQuery(owner=owner, repo=repo, accommodator=token_proxy_accommodator,
                                 opensearch_client=opensearch_client, opensearch_api=opensearch_api,
                                 issue_type=issue_type)
    graphql_query.get_issues_and_issue_timelines()
