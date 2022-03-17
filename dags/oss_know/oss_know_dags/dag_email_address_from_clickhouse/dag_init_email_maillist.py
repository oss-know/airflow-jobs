from airflow.utils.db import provide_session
from airflow.models import XCom
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from oss_know.libs.base_dict.variable_key import NEED_INIT_GITHUB_PROFILES_REPOS, CLICKHOUSE_DRIVER_INFO


@provide_session
def cleanup_xcom(session=None):
    dag_id = 'github_init_maillist_info_from_ck_info_v1'
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


with DAG(
        dag_id='github_init_maillist_info_from_ck_info_v1',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['email_address'],
        on_success_callback=cleanup_xcom
) as dag:
    def start_load_maillist_info_info(ds, **kwargs):
        return 'End start_load_maillist_info_info'


    op_start_load_maillist_info_info = PythonOperator(
        task_id='load_maillist_info_info',
        python_callable=start_load_maillist_info_info,
        provide_context=True
    )


    def load_maillist_info(params, **kwargs):
        from airflow.models import Variable
        from oss_know.libs.email_address import maillist_info
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        maillist_info.load_all_maillist_info(clickhouse_server_info=clickhouse_server_info)
        return 'load_all_maillist_info:::end'


    op_load_maillist_info = PythonOperator(
        task_id='op_load_maillist_info',
        python_callable=load_maillist_info,

        provide_context=True
    )
    op_start_load_maillist_info_info >> op_load_maillist_info
