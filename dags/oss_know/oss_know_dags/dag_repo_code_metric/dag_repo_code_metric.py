from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_GIT_RAW
from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO, DAILY_SYNC_INTERVAL, MYSQL_CONN_INFO, \
    ROUTINELY_UPDATE_INFLUENCE_METRICS_INTERVAL, ROUTINELY_UPDATE_INFLUENCE_METRICS_INCLUDES
from oss_know.libs.metrics.influence_metrics import MetricGroupRoutineCalculation
from oss_know.libs.metrics.repo_code_metrics import RepoCodeMetricRoutineCalculation
from oss_know.libs.util.base import arrange_owner_repo_into_letter_groups
from oss_know.libs.util.clickhouse import get_uniq_owner_repos

sync_interval = Variable.get(ROUTINELY_UPDATE_INFLUENCE_METRICS_INTERVAL, default_var=None)
if not sync_interval:
    sync_interval = Variable.get(DAILY_SYNC_INTERVAL, default_var=None)

mysql_conn_info = Variable.get(MYSQL_CONN_INFO, deserialize_json=True)

with DAG(dag_id='routinely_calculate_repo_code_metrics',  # schedule_interval='*/5 * * * *',
         schedule_interval=sync_interval, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['metrics'], ) as dag:
    uniq_owner_repos = Variable.get(ROUTINELY_UPDATE_INFLUENCE_METRICS_INCLUDES,
                                    deserialize_json=True, default_var=None)
    if not uniq_owner_repos:
        clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        uniq_owner_repos = get_uniq_owner_repos(clickhouse_conn_info, OPENSEARCH_GIT_RAW)

    task_groups_by_capital_letter = arrange_owner_repo_into_letter_groups(uniq_owner_repos)


    def do_calculate_repo_code_metrics_by_routine_class(owner_repo_group):
        calc = MetricGroupRoutineCalculation(RepoCodeMetricRoutineCalculation,
                                             clickhouse_conn_info, mysql_conn_info,
                                             owner_repo_group, 'repo_code_metrics')
        calc.routine()


    for letter, owner_repos in task_groups_by_capital_letter.items():
        op_calculate_repo_code_metrics = PythonOperator(
            task_id=f'op_calculate_repo_code_metrics_{letter}',
            python_callable=do_calculate_repo_code_metrics_by_routine_class,
            trigger_rule='all_done',
            op_kwargs={
                "owner_repo_group": owner_repos
            }
        )
