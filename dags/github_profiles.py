import time
from datetime import datetime
from pprint import pprint
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='github_profile_v1',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['github'],
) as dag:
    # [START howto_operator_python]
    def do_load_github_profile_info(ds, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint("===============kwargs===============")
        pprint(kwargs)
        pprint("====================================")

        print("*****************kwargs*****************")
        print(kwargs)
        print("*****************ds*****************")
        print(ds)

        from airflow.models import Variable
        from libs.github import profiles

        github_tokens = Variable.get("github_infos", deserialize_json=True)
        opensearch_conn_infos = Variable.get("opensearch_conn_infos", deserialize_json=True)

        # print("*****************github_tokens*****************")
        # for token in github_tokens:
        #     print(token)

        load_info = profiles.load_github_profile(github_tokens, opensearch_conn_infos)


        print(load_info)


        # print(opensearch_conn_infos["HOST"])

        return 'Whatever you return gets printed in the logs'

    run_this = PythonOperator(
        task_id='load_github_profile_info',
        python_callable=do_load_github_profile_info,
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

        run_this >> task
    # [END howto_operator_python_kwargs]


