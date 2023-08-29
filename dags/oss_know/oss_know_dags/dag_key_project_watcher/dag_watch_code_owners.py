from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from oss_know.libs.base_dict.variable_key import GIT_SAVE_LOCAL_PATH, CLICKHOUSE_DRIVER_INFO
from oss_know.libs.github.code_owner import LLVMCodeOwnerWatcher, PytorchCodeOwnerWatcher

git_repo_path = Variable.get(GIT_SAVE_LOCAL_PATH, deserialize_json=True)
ck_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)

with DAG(dag_id='dag_watch_code_owners', schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['analysis'], concurrency=5) as dag:
    def do_watch(watcher_class):
        watcher = watcher_class(git_repo_path['PATH'], ck_conn_info)
        watcher.init()
        watcher.watch()


    for cls in [LLVMCodeOwnerWatcher, PytorchCodeOwnerWatcher]:
        owner = cls.OWNER
        repo = cls.REPO
        operator = PythonOperator(
            task_id=f'watch_{owner}_{repo}',
            python_callable=do_watch,
            op_kwargs={
                'watcher_class': cls
            },
        )
