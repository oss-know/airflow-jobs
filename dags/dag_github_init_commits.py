from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import airflow.providers.postgres.hooks.postgres as postgres_hooks



# irflow.providers.postgres.hooks.postgres
# v0.0.1 初始化实现
# v0.0.2 增加set_github_init_commits_check_data 用于设置初始化后更新的point data

with DAG(
        dag_id='github_init_commits_v1',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def scheduler_init_sync_github_commit(ds, **kwargs):
        return 'End::scheduler_init_sync_github_commit'


    op_scheduler_init_sync_github_commit = PythonOperator(
        task_id='scheduler_init_sync_github_commit',
        python_callable=scheduler_init_sync_github_commit
    )


    def do_init_sync_github_commit(params):
        from airflow.models import Variable
        from libs.github import init_commits

        github_tokens = Variable.get("github_tokens", deserialize_json=True)
        opensearch_conn_info = Variable.get("opensearch_conn_data", deserialize_json=True)

        owner = params["owner"]
        repo = params["repo"]
        since = params["since"]
        until = params["until"]

        # 演示从airflow 获取 postgres_hooks 再获取 conn 链接
        # postgres_conn = postgres_hooks.PostgresHook.get_hook("airflow-jobs").get_conn()
        # pg_cursor = postgres_conn.cursor()
        # pg_cursor.execute("select version();")
        # record = pg_cursor.fetchone()
        # print("PostgresRecord:", record)
        # pg_cursor.close()
        # postgres_conn.close()

        postgres_conn = postgres_hooks.PostgresHook.get_hook("airflow-jobs").get_conn()
        with postgres_conn:
            do_init_sync_info = init_commits.init_sync_github_commits(github_tokens,
                                                                      opensearch_conn_info,
                                                                      postgres_conn,
                                                                      owner,
                                                                      repo,
                                                                      since,
                                                                      until)


            print(do_init_sync_info)

        return "END::do_init_sync_github_commit"


    need_do_inti_sync_ops = []

    from airflow.models import Variable

    need_init_sync_github_commits_list = Variable.get("need_init_sync_github_commits_list", deserialize_json=True)

    for now_need_init_sync_github_commits in need_init_sync_github_commits_list:
        op_do_init_sync_github_commit = PythonOperator(
            task_id='do_init_sync_github_commit_{owner}_{repo}'.format(
                owner=now_need_init_sync_github_commits["owner"],
                repo=now_need_init_sync_github_commits["repo"]),
            python_callable=do_init_sync_github_commit,
            op_kwargs={'params': now_need_init_sync_github_commits},
        )
        op_scheduler_init_sync_github_commit >> op_do_init_sync_github_commit
