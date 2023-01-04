from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, MAIL_LISTS

# v0.0.1 It is a mailing list DAG

with DAG(
        dag_id='init_mail_list_archive',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['email'],
) as dag:
    def scheduler_init_mail_list(ds, **kwargs):
        return 'End::scheduler_init_mail_list'


    op_scheduler_init_mail_list = PythonOperator(
        task_id='scheduler_init_mail_list',
        python_callable=scheduler_init_mail_list
    )


    def do_init_mail_list(params):
        from oss_know.libs.maillist.archive import init_archive
        opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
        init_archive(opensearch_conn_info, **params)
        return 'End::init_mail_list'


    mail_lists = Variable.get(MAIL_LISTS, deserialize_json=True)

    for mail_list in mail_lists:
        task_name = mail_list["project_name"]
        for mail in mail_list["mail_lists"]:
            op_do_init_mail_list = PythonOperator(
                task_id=f'init_mail_list_{mail["list_name"]}',
                python_callable=do_init_mail_list,
                op_kwargs={'params': dict({"project_name": task_name}.items() | mail.items())},
            )
            op_scheduler_init_mail_list >> op_do_init_mail_list
