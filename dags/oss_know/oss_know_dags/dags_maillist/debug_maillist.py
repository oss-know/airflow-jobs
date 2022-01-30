from oss_know.libs.maillist.archive import sync_archive
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, GITHUB_TOKENS, NEED_INIT_GITHUB_COMMITS_REPOS, MAIL_LISTS


with DAG(
        dag_id='debug_maillist',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['email'],
) as dag:
    def scheduler_init_debug_maillist(ds, **kwargs):
        return 'End::scheduler_init_debug_maillist'

    op_scheduler_init_debug_maillist = PythonOperator(
        task_id='scheduler_init_debug_maillist',
        python_callable=scheduler_init_debug_maillist
    )

    def do_debug_maillist(params):
        from oss_know.libs.maillist.archive import sync_archive
        opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
        sync_archive(opensearch_conn_info, **params)
        return 'End::debug_maillist'

    need_do_inti_sync_ops = []
    mail_lists = Variable.get(f'{MAIL_LISTS}_debug', deserialize_json=True)

    for mail_list in mail_lists:
        task_name = mail_list["project_name"]
        for mail in mail_list["mail_lists"]:
            op_do_debug_maillist = PythonOperator(
                task_id=f'debug_maillist_{mail["list_name"]}',
            python_callable=do_debug_maillist,
                op_kwargs={'params': dict({"project_name": task_name}.items() | mail.items())},
            )
            op_scheduler_init_debug_maillist >> op_do_debug_maillist
