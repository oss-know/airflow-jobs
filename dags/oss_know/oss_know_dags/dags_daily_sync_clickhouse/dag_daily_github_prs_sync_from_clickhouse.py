from datetime import datetime
from string import ascii_lowercase

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO, SYNC_FROM_CLICKHOUSE_DRIVER_INFO, \
    SYNC_CLICKHOUSE_INTERVAL

from oss_know.libs.clickhouse.sync_clickhouse_data import sync_from_remote_by_repos, union_remote_owner_repos

clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
sync_from_clickhouse_conn_info = Variable.get(SYNC_FROM_CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
sync_interval = Variable.get(SYNC_CLICKHOUSE_INTERVAL, default_var=None)

# Daily sync github_prs data from other clickhouse environment by owner/repo
with DAG(dag_id='daily_github_prs_sync_from_clickhouse',  # schedule_interval='*/5 * * * *',
         schedule_interval=sync_interval, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'daily sync clickhouse'], ) as dag:
    def do_init():
        return 'Start init_daily_github_prs_sync'


    op_init = PythonOperator(task_id='op_init_daily_github_prs_sync_from_clickhouse', python_callable=do_init)


    def do_sync_github_prs_from_clickhouse_by_group(params):
        sync_from_remote_by_repos(clickhouse_conn_info, sync_from_clickhouse_conn_info,
                                  "github_pull_requests",
                                  params.get('owner_repos'))


    all_owner_repos = union_remote_owner_repos(clickhouse_conn_info, sync_from_clickhouse_conn_info,
                                               "github_pull_requests")

    # Init 26 sub groups by letter(to make the task DAG static)
    # Split all tasks into 26 groups by their capital letter, all tasks inside a group are executed sequentially
    # To avoid to many parallel tasks and keep the DAG static
    task_groups_by_capital_letter = {}
    for letter in ascii_lowercase:
        task_groups_by_capital_letter[letter] = []

    for owner_repo_pair in all_owner_repos:
        owner, _ = owner_repo_pair
        task_groups_by_capital_letter[owner[0].lower()].append(owner_repo_pair)

    prev_group = None
    for letter, owner_repos in task_groups_by_capital_letter.items():
        op_sync_github_prs_from_clickhouse_group = PythonOperator(
            task_id=f'op_sync_github_prs_from_clickhouse_group_{letter}',
            python_callable=do_sync_github_prs_from_clickhouse_by_group,
            trigger_rule='all_done',
            op_kwargs={
                "params": {
                    "owner_repos": owner_repos
                }
            }
        )
        if not prev_group:
            op_init >> op_sync_github_prs_from_clickhouse_group
        else:
            prev_group >> op_sync_github_prs_from_clickhouse_group
        prev_group = op_sync_github_prs_from_clickhouse_group
