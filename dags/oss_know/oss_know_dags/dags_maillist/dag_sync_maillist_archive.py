from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, MAIL_LISTS

# v0.0.1 It is a mailing list DAG

with DAG(
        dag_id='init_maillist_archive',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['email'],
) as dag:
    def scheduler_init_sync_maillist(ds, **kwargs):
        return 'End::scheduler_init_sync_maillist'


    op_scheduler_init_sync_maillist = PythonOperator(
        task_id='scheduler_init_sync_maillist',
        python_callable=scheduler_init_sync_maillist
    )


    def do_sync_maillist(params):
        from oss_know.libs.maillist.archive import sync_archive
        opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
        sync_archive(opensearch_conn_info, **params)
        return 'End::sync_maillist'


    need_do_inti_sync_ops = []
    mail_lists = Variable.get(MAIL_LISTS, deserialize_json=True)

    for mail_list in mail_lists:
        task_name = mail_list["project_name"]
        for mail in mail_list["mail_lists"]:
            op_do_sync_maillist = PythonOperator(
                task_id=f'sync_maillist_{mail["list_name"]}',
                python_callable=do_sync_maillist,
                op_kwargs={'params': dict({"project_name": task_name}.items() | mail.items())},
            )
            op_scheduler_init_sync_maillist >> op_do_sync_maillist
