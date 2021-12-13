import time
from datetime import datetime
from pprint import pprint
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
        dag_id='github_commits_v1',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    # [START howto_operator_python]
    def do_sync_github_commit(ds, **kwargs):
        from airflow.models import Variable
        from libs.github import commits

        github_tokens = Variable.get("github_infos", deserialize_json=True)
        opensearch_conn_infos = Variable.get("opensearch_conn_infos", deserialize_json=True)

        sync_params = kwargs.get("params")
        owner = sync_params["owner"]
        repo = sync_params["repo"]
        since = sync_params["since"]
        until = sync_params["until"]

        load_info = commits.sync_github_commits(github_tokens, opensearch_conn_infos, owner, repo, since, until)

        # print(load_info)

        return 'Whatever you return gets printed in the logs'


    do_sync_github_commit = PythonOperator(
        task_id='do_sync_github_commit',
        python_callable=do_sync_github_commit,
    )


    # [END howto_operator_python]

    # [START howto_operator_python_kwargs]
    def my_sleeping_function(random_base):
        """This is a function that will run within the DAG execution"""
        time.sleep(random_base)


    # Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
    for i in range(5):
        task = PythonOperator(
            task_id='sleep_for_' + str(i),
            python_callable=my_sleeping_function,
            op_kwargs={'random_base': float(i) / 10},
        )

        do_sync_github_commit >> task
    # [END howto_operator_python_kwargs]
