from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, GITHUB_TOKENS, NEED_INIT_GITHUB_COMMITS_REPOS, MAIL_LISTS

# irflow.providers.postgres.hooks.postgres
# v0.0.1 初始化实现
# v0.0.2 增加set_github_init_commits_check_data 用于设置初始化后更新的point data

with DAG(
        dag_id='sync_maillist',
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
        op_do_sync_maillist = PythonOperator(
            task_id=f'sync_maillist_{mail_list["project_name"]}',
            python_callable=do_sync_maillist,
            op_kwargs={'params': mail_list},
        )
        op_scheduler_init_sync_maillist >> op_do_sync_maillist
