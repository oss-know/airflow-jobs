from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import MYSQL_CONN_INFO, MYSQL_CREATE_TABLE_DDL
from oss_know.libs.util.mysql_connector import get_mysql_conn

with DAG(
        dag_id='ck_create_mysql_table',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['mysql'],
) as dag:
    def do_create_mysql_table(ddl):
        from airflow.models import Variable
        mysql_connect_info = Variable.get(MYSQL_CONN_INFO, deserialize_json=True)
        mysql_connector = get_mysql_conn(mysql_connect_info)
        cursor = mysql_connector.cursor()
        cursor.execute(ddl)
        return 'do_ck_create_table:::end'


    from airflow.models import Variable

    mysql_table_ddls = Variable.get(MYSQL_CREATE_TABLE_DDL, deserialize_json=True)
    for table_info in mysql_table_ddls:
        op_do_create_mysql_table = PythonOperator(
            task_id=f'do_create_mysql_table_{table_info["table_name"]}',
            python_callable=do_create_mysql_table,
            op_kwargs={'ddl': table_info["ddl"]},
        )
