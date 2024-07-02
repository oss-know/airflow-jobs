# company_email_domain_map

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO
from oss_know.libs.middle_tables.code_owners_makeup import makeup_periods
from oss_know.libs.util.clickhouse_driver import CKServer

clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
ck_client = CKServer(host=clickhouse_conn_info.get("HOST"),
                     port=clickhouse_conn_info.get("PORT"),
                     user=clickhouse_conn_info.get("USER"),
                     password=clickhouse_conn_info.get("PASSWD"),
                     database=clickhouse_conn_info.get("DATABASE"),
                     settings={
                         "max_execution_time": 1000000,
                     },
                     kwargs={
                         "connect_timeout": 200,
                         "send_receive_timeout": 6000,
                         "sync_request_timeout": 100,
                     })

create_table_sql = '''
create table if not exists code_owner_history_by_year_month
(
    year_month Int64,
    id_type     String,
    owner_id String,
    owner String,
    repo String
)
    engine = Memory;
'''

repo_infos = [
    {
        'owner': 'pytorch',
        'repo': 'pytorch',
        'id_type': 'github_login',
    },
    {
        'owner': 'llvm',
        'repo': 'llvm-project',
        'id_type': 'email',
    },
]

with DAG(dag_id='dag_makeup_code_owners_of_empty_periods',  # schedule_interval='*/5 * * * *',
         schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'middle table'], ) as dag:
    def do_prepare_memory_table():
        ck_client.execute_no_params(create_table_sql)
        ck_client.execute_no_params('truncate table code_owner_history_by_year_month')


    def do_makeup_project_periods(owner, repo, id_type):
        rows = makeup_periods(owner, repo, id_type, ck_client)
        ck_client.execute('insert into table code_owner_history_by_year_month values', rows)


    op_prepare_memory_table = PythonOperator(
        task_id=f'op_prepare_memory_table',
        python_callable=do_prepare_memory_table,
        op_kwargs={}
    )

    for repo_info in repo_infos:
        owner = repo_info['owner']
        repo = repo_info['repo']
        id_type = repo_info['id_type']

        op_makeup_periods = PythonOperator(
            task_id=f'op_makeup_{owner}_{repo}_{id_type}',
            python_callable=do_makeup_project_periods,
            op_kwargs=repo_info
        )

        op_prepare_memory_table >> op_makeup_periods
