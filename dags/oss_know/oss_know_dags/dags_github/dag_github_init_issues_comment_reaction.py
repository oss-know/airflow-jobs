from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import NEED_INIT_GITHUB_ISSUES_COMMENT_REACTION_REPOS, \
    GITHUB_TOKENS, OPENSEARCH_CONN_DATA, PROXY_CONFS
from oss_know.libs.github import init_issues_comment_reaction
from oss_know.libs.util.proxy import GithubTokenProxyAccommodator, ProxyServiceProvider, \
    make_accommodator

# v0.0.1
with DAG(
        dag_id='dag_github_init_issues_comment_reaction',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def scheduler_init_github_issues_comment_reaction():
        return 'End:github_init_issues_comment_reaction'


    op_scheduler_init_github_issues_comment_reaction = PythonOperator(
        task_id='op_scheduler_init_github_issues_comment_reaction',
        python_callable=scheduler_init_github_issues_comment_reaction
    )


    def do_init_github_issues_comment_reaction(params):
        opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)

        github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
        proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
        proxy_accommodator = make_accommodator(github_tokens, proxy_confs, ProxyServiceProvider.Kuai,
                                               GithubTokenProxyAccommodator.POLICY_FIXED_MAP)

        owner = params["owner"]
        repo = params["repo"]

        init_issues_comment_reaction.init_github_issues_comment_reaction(opensearch_conn_info,
                                                                         owner,
                                                                         repo,
                                                                         proxy_accommodator)

        return params


    need_do_init_ops = []

    need_init_github_issues_comment_reaction_repos = Variable.get(NEED_INIT_GITHUB_ISSUES_COMMENT_REACTION_REPOS,
                                                                  deserialize_json=True)

    for need_init_github_issues_comment_reaction_repo in need_init_github_issues_comment_reaction_repos:
        op_do_init_github_issues_comment_reaction = PythonOperator(
            task_id='op_do_init_github_issues_comment_reaction_{owner}_{repo}'.format(
                owner=need_init_github_issues_comment_reaction_repo["owner"],
                repo=need_init_github_issues_comment_reaction_repo["repo"]),
            python_callable=do_init_github_issues_comment_reaction,
            op_kwargs={'params': need_init_github_issues_comment_reaction_repo},
        )
        op_scheduler_init_github_issues_comment_reaction >> op_do_init_github_issues_comment_reaction
