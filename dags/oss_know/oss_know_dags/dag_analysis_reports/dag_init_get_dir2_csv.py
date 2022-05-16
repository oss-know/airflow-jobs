# -*-coding:utf-8-*-
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# git_init_sync_v0.0.3
from oss_know.libs.analysis_report.init_get_dir2_contribute_data_v1 import get_dir2_contribute_data
from oss_know.libs.analysis_report.int_get_dir2_csv import get_data_csv
from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO
from oss_know.libs.util.clickhouse_driver import CKServer

with DAG(
        dag_id='dag_init_get_dir2_csv.py',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['analysis'],
) as dag:
    def init_sync_get_dir2_csv(ds, **kwargs):
        return 'Start init_sync_get_dir2_csv'


    op_init_sync_get_dir2_csv = PythonOperator(
        task_id='init_sync_git_info',
        python_callable=init_sync_get_dir2_csv,
    )


    def do_sync_get_dir2_csv(params):
        from airflow.models import Variable
        department = params["department"]
        ck_conn_info = params["ck_conn_info"]
        get_data_csv(department=department,ck_conn_info=ck_conn_info)

        return 'do_sync_get_dir2_csv:::end'


    from airflow.models import Variable

    ck_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
    ck = CKServer(host=ck_conn_info["HOST"],
                  port=ck_conn_info["PORT"],
                  user=ck_conn_info["USER"],
                  password=ck_conn_info["PASSWD"],
                  database=ck_conn_info["DATABASE"])
    get_departments_sql = f"select department from dir2_analysis_v1 group by department"
    departments = ck.execute_no_params(get_departments_sql)
    for department in departments:
        ck_conn_info_and_department = {"ck_conn_info":ck_conn_info,"department":department[0]}
        op_do_init_sync_get_dir2_csv = PythonOperator(
            task_id=f'project_line_{department[0]}',
            python_callable=do_sync_get_dir2_csv,
            op_kwargs={'params': ck_conn_info_and_department},
        )
        op_init_sync_get_dir2_csv >> op_do_init_sync_get_dir2_csv
