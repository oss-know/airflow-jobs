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
        dag_id='dag_analysis_for_dashboard_by_repo',
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


    def do_get_dir_label(params):
        from oss_know.libs.metrics.init_analysis_data_for_dashboard import get_dir_n
        owner = params["owner"]
        repo = params["repo"]
        from airflow.models import Variable

        clickhouse_conn = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        get_dir_n(ck_con=clickhouse_conn,owner=owner, repo=repo)
        return "end::do_analysis_for_dashboard"


    def do_get_alter_files_count(params):
        from oss_know.libs.metrics.init_analysis_data_for_dashboard import get_alter_files_count
        owner = params["owner"]
        repo = params["repo"]
        from airflow.models import Variable

        clickhouse_conn = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        get_alter_files_count(ck_con=clickhouse_conn,owner=owner, repo=repo)
        return "end::do_analysis_for_dashboard"


    def do_get_dir_contributer_count(params):
        from oss_know.libs.metrics.init_analysis_data_for_dashboard import get_dir_contributer_count
        owner = params["owner"]
        repo = params["repo"]
        from airflow.models import Variable

        clickhouse_conn = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        get_dir_contributer_count(ck_con=clickhouse_conn,owner=owner, repo=repo)
        return "end::do_analysis_for_dashboard"


    def do_get_alter_file_count_by_dir_email_domain(params):
        from oss_know.libs.metrics.init_analysis_data_for_dashboard import get_alter_file_count_by_dir_email_domain
        owner = params["owner"]
        repo = params["repo"]
        from airflow.models import Variable

        clickhouse_conn = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        get_alter_file_count_by_dir_email_domain(ck_con=clickhouse_conn,owner=owner, repo=repo)
        return "end::do_analysis_for_dashboard"


    def do_get_contributer_by_dir_email_domain(params):
        from oss_know.libs.metrics.init_analysis_data_for_dashboard import get_contributer_by_dir_email_domain
        owner = params["owner"]
        repo = params["repo"]
        from airflow.models import Variable

        clickhouse_conn = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        get_contributer_by_dir_email_domain(ck_con=clickhouse_conn,owner=owner, repo=repo)
        return "end::do_analysis_for_dashboard"


    def do_get_tz_distribution(params):
        from oss_know.libs.metrics.init_analysis_data_for_dashboard import get_tz_distribution
        owner = params["owner"]
        repo = params["repo"]
        from airflow.models import Variable

        clickhouse_conn = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        get_tz_distribution(ck_con=clickhouse_conn,owner=owner, repo=repo)
        return "end::do_analysis_for_dashboard"


    results = Variable.get("dashboard_repo_list", deserialize_json=True)

    for repo_list in results:
        owner = repo_list["owner"]
        repo = repo_list["repo"]
        op_do_analysis_for_dashboard = PythonOperator(
            task_id=f'do_analysis_for_dashboard_owner_{owner}_repo_{repo}',
            python_callable=do_get_dir_label,
            op_kwargs={'params': repo_list},
        )
        op_do_analysis = PythonOperator(
            task_id=f'do_analysis_owner_{owner}_repo_{repo}',
            python_callable=do_get_alter_files_count,
            op_kwargs={'params': repo_list}

        )
        op_do_analysis2 = PythonOperator(
            task_id=f'do_analysis2_owner_{owner}_repo_{repo}',
            python_callable=do_get_dir_contributer_count,
            op_kwargs={'params': repo_list}

        )
        op_do_analysis3 = PythonOperator(
            task_id=f'do_analysis3_owner_{owner}_repo_{repo}',
            python_callable=do_get_alter_file_count_by_dir_email_domain,
            op_kwargs={'params': repo_list}

        )

        op_do_analysis4 = PythonOperator(
            task_id=f'do_analysis4_owner_{owner}_repo_{repo}',
            python_callable=do_get_contributer_by_dir_email_domain,
            op_kwargs={'params': repo_list}

        )

        op_do_analysis5 = PythonOperator(
            task_id=f'do_analysis5_owner_{owner}_repo_{repo}',
            python_callable=do_get_tz_distribution,
            op_kwargs={'params': repo_list}
        )
        op_init_analysis_for_dashboard >> op_do_analysis_for_dashboard >> op_do_analysis
        op_init_analysis_for_dashboard >> op_do_analysis_for_dashboard >> op_do_analysis2
        op_init_analysis_for_dashboard >> op_do_analysis_for_dashboard >> op_do_analysis3
        op_init_analysis_for_dashboard >> op_do_analysis_for_dashboard >> op_do_analysis4
        op_init_analysis_for_dashboard >> op_do_analysis_for_dashboard >> op_do_analysis5
        # op_init_analysis_for_dashboard  >> op_do_analysis
        # op_init_analysis_for_dashboard  >> op_do_analysis2
        # op_init_analysis_for_dashboard  >> op_do_analysis3
        # op_init_analysis_for_dashboard  >> op_do_analysis4
        # op_init_analysis_for_dashboard  >> op_do_analysis5
