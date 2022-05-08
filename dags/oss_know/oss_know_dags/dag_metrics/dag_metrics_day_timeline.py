import time
from datetime import datetime
from loguru import logger
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# statistics_metrics_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO
from oss_know.libs.metrics.init_metrics_day_timeline import get_metries_timeline_by_repo
from oss_know.libs.metrics.init_statistics_metrics import statistics_metrics, statistics_activities
from oss_know.libs.util.clickhouse_driver import CKServer

with DAG(
        dag_id='ck_metrics_day_timeline',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['analysis'],
) as dag:
    def init_metrics_day_timeline(ds, **kwargs):
        return 'Start init_ck_metrics_day_timeline'


    op_init_metrics_day_timeline = PythonOperator(
        task_id='init_ck_metrics_day_timeline',
        python_callable=init_metrics_day_timeline,
    )


    def do_metrics_day_timeline(params):
        # owner = params["owner"]
        # repo = params["repo"]
        owner = params[0]
        repo = params[1]
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        ck = CKServer(host='192.168.8.152', port=9000, user='default', password='default',
                      database='default')
        table_name = 'metrics_day_timeline'
        logger.info(
            f"ALTER TABLE {table_name}_local ON CLUSTER replicated DELETE WHERE owner='{owner}' and repo='{repo}'.......")
        ck.execute_no_params(
            f"ALTER TABLE {table_name}_local ON CLUSTER replicated DELETE WHERE owner='{owner}' and repo='{repo}';")

        count = \
        ck.execute_no_params(f"select count() from {table_name} WHERE owner='{owner}' and repo='{repo}'")[0][0]
        while count != 0:
            logger.info("wait 10 sec.......")
            time.sleep(10)
            count = ck.execute_no_params(f"select count() from {table_name} WHERE owner='{owner}' and repo='{repo}'")[0][0]
        logger.info("Deleted successfully")
        get_metries_timeline_by_repo(ck=ck,owner=owner,repo=repo,table_name=table_name)
        return "end::do_ck_metrics_day_timeline"




    # from airflow.models import Variable
    # repo_lists = Variable.get("metrics_day_timeline_repo_list", deserialize_json=True)
    # #[{"owner":"","repo":""}]
    # # print(repo_lists)
    #
    # for repo_list in repo_lists:
    #     owner = repo_list["owner"]
    #     repo = repo_list["repo"]
    #     op_do_metrics_day_timeline = PythonOperator(
    #         task_id=f'do_metrics_day_timeline_owner_{owner}_repo_{repo}',
    #         python_callable=do_metrics_day_timeline,
    #         op_kwargs={'params': repo_list},
    #     )
    #     op_init_metrics_day_timeline >> op_do_metrics_day_timeline
    ck = CKServer(host='192.168.8.152', port=9000, user='default', password='default',
                  database='default')
    results = ck.execute_no_params("select search_key__owner,search_key__repo from gits group by search_key__owner,search_key__repo order by count() desc")
    #[()]fdsaXMODIFIERS=@im=fcitxfdsa
    for repo_list in results:
        owner = repo_list[0]
        repo = repo_list[1]
        op_do_metrics_day_timeline = PythonOperator(
            task_id=f'do_metrics_day_timeline_owner_{owner}_repo_{repo}',
            python_callable=do_metrics_day_timeline,
            op_kwargs={'params': repo_list},
        )
        op_init_metrics_day_timeline >> op_do_metrics_day_timeline