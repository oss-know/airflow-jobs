from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES, \
    OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS, OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE
from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, GITHUB_TOKENS, PROXY_CONFS, \
    DAILY_SYNC_GITHUB_ISSUES_EXCLUDES, CLICKHOUSE_DRIVER_INFO, CK_TABLE_DEFAULT_VAL_TPLT, \
    DAILY_SYNC_GITHUB_ISSUES_INCLUDES, DAILY_SYNC_INTERVAL, DAILY_GITHUB_ISSUES_SYNC_INTERVAL
from oss_know.libs.github import sync_issues, sync_issues_comments, sync_issues_timelines
from oss_know.libs.util.base import get_opensearch_client, arrange_owner_repo_into_letter_groups
from oss_know.libs.util.data_transfer import sync_clickhouse_from_opensearch, sync_clickhouse_repos_from_opensearch
from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.util.proxy import GithubTokenProxyAccommodator, ProxyServiceProvider, make_accommodator

OP_SYNC_ISSUE_PREFIX = 'op_sync_github_issues'

sync_interval = Variable.get(DAILY_GITHUB_ISSUES_SYNC_INTERVAL, default_var=None)
if not sync_interval:
    sync_interval = Variable.get(DAILY_SYNC_INTERVAL, default_var=None)

with DAG(
        dag_id='daily_github_issues_sync_v2',
        schedule_interval=sync_interval,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github', 'daily sync'],
) as dag:
    opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)

    clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
    table_templates = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
    github_issue_table_template = table_templates.get(OPENSEARCH_INDEX_GITHUB_ISSUES)
    issue_comment_table_template = table_templates.get(OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS)
    issue_timeline_table_template = table_templates.get(OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE)

    github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
    proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
    proxy_accommodator = make_accommodator(github_tokens, proxy_confs, ProxyServiceProvider.Kuai,
                                           GithubTokenProxyAccommodator.POLICY_FIXED_MAP)


    def do_sync_github_issues_opensearch_group(owner_repo_group):
        # A list of object that contains: owner, repo and the related issue numbers
        issue_number_infos = []

        for item in owner_repo_group:
            owner = item['owner']
            repo = item['repo']
            issues_numbers, _ = sync_issues.sync_github_issues(
                owner=owner, repo=repo,
                opensearch_conn_info=opensearch_conn_info,
                token_proxy_accommodator=proxy_accommodator)
            issue_number_infos.append({
                'owner': owner,
                'repo': repo,
                'issues_numbers': issues_numbers,
            })
        return issue_number_infos


    def do_sync_github_issues_clickhouse_group(owner_repo_group):
        for item in owner_repo_group:
            owner = item['owner']
            repo = item['repo']
            sync_clickhouse_from_opensearch(owner, repo,
                                            OPENSEARCH_INDEX_GITHUB_ISSUES, opensearch_conn_info,
                                            OPENSEARCH_INDEX_GITHUB_ISSUES, clickhouse_conn_info,
                                            github_issue_table_template)


    def do_sync_github_issues_comments_opensearch_group(group_letter, **kwargs):
        ti = kwargs['ti']
        task_ids = f'{OP_SYNC_ISSUE_PREFIX}_opensearch_group_{group_letter}'
        all_issue_numbers = ti.xcom_pull(task_ids=task_ids)

        for item in all_issue_numbers:
            owner = item['owner']
            repo = item['repo']
            issues_numbers = item['issues_numbers']
            sync_issues_comments.sync_github_issues_comments(
                opensearch_conn_info=opensearch_conn_info,
                owner=owner,
                repo=repo,
                token_proxy_accommodator=proxy_accommodator,
                issues_numbers=issues_numbers
            )


    def do_sync_github_issues_comments_clickhouse_group(owner_repo_group):
        sync_clickhouse_repos_from_opensearch(owner_repo_group,
                                              OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS, opensearch_conn_info,
                                              OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS, clickhouse_conn_info,
                                              issue_comment_table_template)


    def do_sync_github_issues_timelines_opensearch_group(group_letter, **kwargs):
        ti = kwargs['ti']
        task_ids = f'{OP_SYNC_ISSUE_PREFIX}_opensearch_group_{group_letter}'
        issue_number_infos = ti.xcom_pull(task_ids=task_ids)

        for item in issue_number_infos:
            owner = item['owner']
            repo = item['repo']
            issues_numbers = item['issues_numbers']

            sync_issues_timelines.sync_github_issues_timelines(
                opensearch_conn_info=opensearch_conn_info,
                owner=owner,
                repo=repo,
                token_proxy_accommodator=proxy_accommodator,
                issues_numbers=issues_numbers)


    def do_sync_github_issues_timeline_clickhouse_group(owner_repo_group):
        sync_clickhouse_repos_from_opensearch(owner_repo_group,
                                              OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE, opensearch_conn_info,
                                              OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE, clickhouse_conn_info,
                                              issue_timeline_table_template)


    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    opensearch_api = OpensearchAPI()

    uniq_owner_repos = []
    includes = Variable.get(DAILY_SYNC_GITHUB_ISSUES_INCLUDES, deserialize_json=True, default_var=None)
    if not includes:
        excludes = Variable.get(DAILY_SYNC_GITHUB_ISSUES_EXCLUDES, deserialize_json=True, default_var=None)
        uniq_owner_repos = opensearch_api.get_uniq_owner_repos(opensearch_client, OPENSEARCH_INDEX_GITHUB_ISSUES,
                                                               excludes)
    else:
        for owner_repo_str, origin in includes.items():
            owner, repo = owner_repo_str.split('::')
            uniq_owner_repos.append({
                'owner': owner,
                'repo': repo,
                'origin': origin
            })

    task_groups_by_capital_letter = arrange_owner_repo_into_letter_groups(uniq_owner_repos)
    for letter, owner_repos in task_groups_by_capital_letter.items():
        op_sync_github_issues_opensearch_group = PythonOperator(
            task_id=f'{OP_SYNC_ISSUE_PREFIX}_opensearch_group_{letter}',
            python_callable=do_sync_github_issues_opensearch_group,
            op_kwargs={'owner_repo_group': owner_repos},
            provide_context=True,
        )
        # op_sync_github_issues
        op_sync_github_issues_clickhouse_group = PythonOperator(
            task_id=f'{OP_SYNC_ISSUE_PREFIX}_clickhouse_group_{letter}',
            python_callable=do_sync_github_issues_clickhouse_group,
            op_kwargs={'owner_repo_group': owner_repos},
            provide_context=True,
        )

        op_sync_github_issues_comments_opensearch_group = PythonOperator(
            task_id=f'{OP_SYNC_ISSUE_PREFIX}_comments_opensearch_group_{letter}',
            python_callable=do_sync_github_issues_comments_opensearch_group,
            op_kwargs={'group_letter': letter},
            # provide_context=True,
        )
        op_sync_github_issues_comments_clickhouse_group = PythonOperator(
            task_id=f'{OP_SYNC_ISSUE_PREFIX}_comments_clickhouse_group{letter}',
            python_callable=do_sync_github_issues_comments_clickhouse_group,
            op_kwargs={'owner_repo_group': owner_repos},
            # provide_context=True,
        )

        op_sync_github_issues_timelines_opensearch_group = PythonOperator(
            task_id=f'{OP_SYNC_ISSUE_PREFIX}_timelines_opensearch_group_{letter}',
            python_callable=do_sync_github_issues_timelines_opensearch_group,
            op_kwargs={'group_letter': letter},
            # provide_context=True,
        )
        op_sync_github_issues_timelines_clickhouse_group = PythonOperator(
            task_id=f'{OP_SYNC_ISSUE_PREFIX}_timelines_clickhouse_group_{letter}',
            python_callable=do_sync_github_issues_timeline_clickhouse_group,
            op_kwargs={'owner_repo_group': owner_repos},
            # provide_context=True,
        )

        op_sync_github_issues_opensearch_group >> op_sync_github_issues_clickhouse_group

        op_sync_github_issues_opensearch_group >> op_sync_github_issues_comments_opensearch_group
        op_sync_github_issues_opensearch_group >> op_sync_github_issues_timelines_opensearch_group

        op_sync_github_issues_comments_opensearch_group >> op_sync_github_issues_comments_clickhouse_group
        op_sync_github_issues_timelines_opensearch_group >> op_sync_github_issues_timelines_clickhouse_group
