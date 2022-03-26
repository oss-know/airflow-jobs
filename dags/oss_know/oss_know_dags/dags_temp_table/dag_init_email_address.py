from airflow.utils.db import provide_session
from airflow.models import XCom
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from oss_know.libs.base_dict.variable_key import NEED_INIT_GITHUB_PROFILES_REPOS, CLICKHOUSE_DRIVER_INFO


@provide_session
def cleanup_xcom(session=None):
    dag_id = 'github_init_temp_gits_by_company_tplt_info_v1'
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


with DAG(
        dag_id='github_init_temp_gits_by_company_tplt_info_v1',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['email_address'],
        on_success_callback=cleanup_xcom
) as dag:
    def load_temp_gits_by_company(params, **kwargs):
        owners = []
        # {"owners": ["systemd", "kubernetes"]}
        # print("=================================")
        # print(type(params))
        # print(params["owners"])
        if params:
            owners = params["owners"]
        from airflow.models import Variable
        from oss_know.libs.email_address import init_temp_gits_by_company_tplt
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        init_temp_gits_by_company_tplt.load_all_temp_gits_by_company(clickhouse_server_info=clickhouse_server_info,
                                                                     owners=owners)
        return 'load_all_temp_gits_by_company:::end'


    op_start_load_temp_gits_by_company_info = PythonOperator(
        task_id='load_temp_gits_by_company_info',
        python_callable=load_temp_gits_by_company,

    )

    op_start_load_temp_gits_by_company_info
