from airflow.utils.db import provide_session
from airflow.models import XCom
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from oss_know.libs.base_dict.variable_key import NEED_INIT_GITHUB_PROFILES_REPOS


@provide_session
def cleanup_xcom(session=None):
    dag_id = 'github_init_email_info_v1'
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


with DAG(
        dag_id='github_init_email_info_v1',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['email'],
        on_success_callback=cleanup_xcom
) as dag:
    def start_load_email_info(ds, **kwargs):
        return 'End start_load_email_info'


    op_start_load_email_info = PythonOperator(
        task_id='load_email_info',
        python_callable=start_load_email_info,
        provide_context=True
    )


    def load_email_address(params, **kwargs):
        from airflow.models import Variable

        opensearch_conn_infos = Variable.get("opensearch_conn_data", deserialize_json=True)
        from oss_know.libs.email_address import init_email_address
        init_email_address.load_email_address(opensearch_conn_infos)

    op_load_email_address = PythonOperator(
        task_id='op_load_email_address',
        python_callable=load_email_address,

        provide_context=True
    )
    op_start_load_email_info >> op_load_email_address
