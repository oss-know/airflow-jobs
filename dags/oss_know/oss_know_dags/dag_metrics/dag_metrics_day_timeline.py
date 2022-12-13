import time
from datetime import datetime
from oss_know.libs.util.log import logger
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# statistics_metrics_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO

from oss_know.libs.metrics.init_statistics_metrics import statistics_metrics, statistics_activities
from oss_know.libs.util.clickhouse_driver import CKServer

with DAG(
        dag_id='ck_metrics_day_timeline',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['analysis'],
) as dag:
    def init_metrics_day_timeline(ds, **kwargs):
        return 'Start init_ck_metrics_day_timeline'


    op_init_metrics_day_timeline = PythonOperator(
        task_id='init_ck_metrics_day_timeline',
        python_callable=init_metrics_day_timeline,
    )


    def do_metrics_timeline(params):
        from oss_know.libs.metrics.init_metrics_day_timeline import get_metries_day_timeline_by_repo, \
            get_metries_month_timeline_by_repo, get_metries_year_timeline_by_repo
        owner = params['owner']
        repo = params['repo']
        time_type = params['time_type']
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        host = clickhouse_server_info["HOST"]
        port = clickhouse_server_info["PORT"]
        user = clickhouse_server_info["USER"]
        password = clickhouse_server_info["PASSWORD"]
        database = clickhouse_server_info["DATABASE"]
        ck = CKServer(host=host, port=port, user=user, password=password,
                      database=database)
        if time_type == 'day':
            table_name = 'metrics_day_timeline'
        elif time_type == 'month':
            table_name = 'metrics_month_timeline'
        else:
            table_name = 'metrics_year_timeline'
        logger.info(
            f"ALTER TABLE {table_name}_local ON CLUSTER replicated DELETE WHERE owner='{owner}' and repo='{repo}'.......")
        ck.execute_no_params(
            f"ALTER TABLE {table_name}_local ON CLUSTER replicated DELETE WHERE owner='{owner}' and repo='{repo}';")

        count = \
            ck.execute_no_params(f"select count() from {table_name} WHERE owner='{owner}' and repo='{repo}'")[0][0]
        while count != 0:
            logger.info("wait 10 sec.......")
            time.sleep(10)
            count = \
            ck.execute_no_params(f"select count() from {table_name} WHERE owner='{owner}' and repo='{repo}'")[0][0]
        logger.info("Deleted successfully")
        if time_type == 'day':
            get_metries_day_timeline_by_repo(ck=ck, owner=owner, repo=repo, table_name=table_name)
        elif time_type == 'month':
            get_metries_month_timeline_by_repo(ck=ck, owner=owner, repo=repo, table_name=table_name)
        else:
            get_metries_year_timeline_by_repo(ck=ck, owner=owner, repo=repo, table_name=table_name)

        return "end::do_ck_metrics_day_timeline"


    results = Variable.get("calculate_metrics_by_day_repos", deserialize_json=True)
    # [()]fdsaXMODIFIERS=@im=fcitxfdsa
    for repo_list in results:
        owner = repo_list['owner']
        repo = repo_list['repo']
        time_type = repo_list['time_type']
        op_do_metrics_day_timeline = PythonOperator(
            task_id=f'do_metrics_day_timeline_owner_{owner}_repo_{repo}_time_type_{time_type}',
            python_callable=do_metrics_timeline,
            op_kwargs={'params': repo_list},
        )
        op_init_metrics_day_timeline >> op_do_metrics_day_timeline
