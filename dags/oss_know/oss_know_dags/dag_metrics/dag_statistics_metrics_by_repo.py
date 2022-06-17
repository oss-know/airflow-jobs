from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# statistics_metrics_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO
from oss_know.libs.metrics.init_statistics_metrics import statistics_metrics, statistics_activities, \
    statistics_metrics_by_repo
from oss_know.libs.util.clickhouse_driver import CKServer

with DAG(
        dag_id='ck_statistics_metrics_by_repo',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['analysis'],
        concurrency=5
) as dag:
    def init_statistics_metrics(ds, **kwargs):
        return 'Start init_statistics_metrics'


    op_init_statistics_metrics = PythonOperator(
        task_id='init_statistics_metrics',
        python_callable=init_statistics_metrics,
    )


    def do_statistics_metrics(params):
        owner = params["owner"]
        repo = params["repo"]
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        statistics_metrics_by_repo(clickhouse_server_info=clickhouse_server_info, owner=owner, repo=repo)
        return "end::do_statistics_metrics"


    def do_statistics_activities(params):
        owner = params["owner"]
        repo = params["repo"]
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        statistics_activities(clickhouse_server_info=clickhouse_server_info,
                              owner=owner,
                              repo=repo)
        return "end::do_statistics_activities"


    owner_repos = Variable.get("calculate_metrics_repo_list", deserialize_json=True)
    for owner_repo in owner_repos:
        owner = owner_repo["owner"]
        repo = owner_repo["owner"]
        op_do_statistics_metrics = PythonOperator(
            task_id=f'do_statistics_metrics_owner_{owner}_repo_{repo}',
            python_callable=do_statistics_metrics,
            op_kwargs={'params': owner_repo}
        )
        op_do_statistics_activities = PythonOperator(
            task_id=f'do_statistics_activities_owner_{owner}_repo_{repo}',
            python_callable=do_statistics_activities,
            op_kwargs={'params': owner_repo}
        )

        op_init_statistics_metrics >> op_do_statistics_metrics >> op_do_statistics_activities
