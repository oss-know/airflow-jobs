import time
from datetime import datetime
from loguru import logger
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# statistics_metrics_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO

from oss_know.libs.metrics.init_metrics_day_timeline import get_metries_day_timeline_by_repo
from oss_know.libs.metrics.init_statistics_metrics import statistics_metrics, statistics_activities
from oss_know.libs.util.clickhouse_driver import CKServer

with DAG(
        dag_id='dag_analysis_for_dashboard',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['analysis'],
        concurrency=5
) as dag:
    def init_analysis_for_dashboard(ds, **kwargs):
        return 'Start init_analysis_for_dashboard'


    op_init_analysis_for_dashboard = PythonOperator(
        task_id='init_analysis_for_dashboard',
        python_callable=init_analysis_for_dashboard,
    )


    def do_analysis_for_dashboard(params):
        from oss_know.libs.metrics.init_analysis_data_for_dashboard import get_dir_n
        owner = params["owner"]
        repo = params["repo"]
        from airflow.models import Variable

        clickhouse_conn = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)

        get_dir_n(owner=owner, repo=repo,ck_con=clickhouse_conn)
        return "end::do_analysis_for_dashboard"


    def do_analysis(params):
        from oss_know.libs.metrics.init_analysis_data_for_dashboard import get_alter_files_count
        from airflow.models import Variable

        clickhouse_conn = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        get_alter_files_count(ck_con=clickhouse_conn)
        return "end::do_analysis_for_dashboard"





    def do_analysis2(params):
        from oss_know.libs.metrics.init_analysis_data_for_dashboard import get_dir_contributer_count
        from airflow.models import Variable

        clickhouse_conn = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        get_dir_contributer_count(ck_con=clickhouse_conn)
        return "end::do_analysis_for_dashboard"





    def do_analysis3(params):
        from oss_know.libs.metrics.init_analysis_data_for_dashboard import get_alter_file_count_by_dir_email_domain
        from airflow.models import Variable

        clickhouse_conn = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        get_alter_file_count_by_dir_email_domain(ck_con=clickhouse_conn)
        return "end::do_analysis_for_dashboard"





    def do_analysis4(params):
        from oss_know.libs.metrics.init_analysis_data_for_dashboard import get_contributer_by_dir_email_domain
        from airflow.models import Variable

        clickhouse_conn = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        get_contributer_by_dir_email_domain(ck_con=clickhouse_conn)
        return "end::do_analysis_for_dashboard"




    def do_analysis5(params):
        from oss_know.libs.metrics.init_analysis_data_for_dashboard import get_tz_distribution
        from airflow.models import Variable

        clickhouse_conn = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        get_tz_distribution(ck_con=clickhouse_conn)
        return "end::do_analysis_for_dashboard"



    op_do_analysis = PythonOperator(
        task_id=f'do_analysis',
        python_callable=do_analysis

    )
    op_do_analysis2 = PythonOperator(
        task_id=f'do_analysis2',
        python_callable=do_analysis2

    )
    op_do_analysis3 = PythonOperator(
        task_id=f'do_analysis3',
        python_callable=do_analysis3

    )

    op_do_analysis4 = PythonOperator(
        task_id=f'do_analysis4',
        python_callable=do_analysis4

    )

    op_do_analysis5 = PythonOperator(
        task_id=f'do_analysis5',
        python_callable=do_analysis5

    )

    results = Variable.get("dashboard_repo_list", deserialize_json=True)

    for repo_list in results:
        owner = repo_list["owner"]
        repo = repo_list["repo"]
        op_do_analysis_for_dashboard = PythonOperator(
            task_id=f'do_analysis_for_dashboard_owner_{owner}_repo_{repo}',
            python_callable=do_analysis_for_dashboard,
            op_kwargs={'params': repo_list},
        )
        op_init_analysis_for_dashboard >> op_do_analysis_for_dashboard
        # op_init_analysis_for_dashboard >> op_do_analysis_for_dashboard >> op_do_analysis
        # op_init_analysis_for_dashboard >> op_do_analysis_for_dashboard >> op_do_analysis2
        # op_init_analysis_for_dashboard >> op_do_analysis_for_dashboard >> op_do_analysis3
        # op_init_analysis_for_dashboard >> op_do_analysis_for_dashboard >> op_do_analysis4
