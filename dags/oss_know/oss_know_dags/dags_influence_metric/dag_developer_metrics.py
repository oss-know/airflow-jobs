from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_GIT_RAW
from oss_know.libs.util.clickhouse import get_uniq_owner_repos
from oss_know.libs.util.log import logger
from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO, DAILY_SYNC_INTERVAL, \
    MYSQL_CONN_INFO, ROUTINELY_UPDATE_INFLUENCE_METRICS_INTERVAL
from oss_know.libs.metrics.influence_metrics import MetricGroupRoutineCalculation
from oss_know.libs.metrics.developer_metrics import *

# sync_interval = Variable.get(ROUTINELY_UPDATE_SAMPLE_INFLUENCE_METRICS_INTERVAL, default_var=None)
# if not sync_interval:
#     sync_interval = Variable.get(DAILY_SYNC_INTERVAL, default_var=None)

mysql_conn_info = Variable.get(MYSQL_CONN_INFO, deserialize_json=True)

with DAG(dag_id='routinely_calculate_developer_metrics',  # schedule_interval='*/5 * * * *',
         schedule_interval=None,
         start_date=datetime(2021, 1, 1), catchup=False,
         tags=['metrics'], ) as dag:
    uniq_owner_repos = Variable.get(ROUTINELY_UPDATE_INFLUENCE_METRICS_INTERVAL, deserialize_json=True)
    clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)

    if not uniq_owner_repos:
        # 如果没有在variable中指定需要计算metrics的项目则从库中获取全量的去重后的owner_repo 计算
        clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        uniq_owner_repos = get_uniq_owner_repos(clickhouse_conn_info, OPENSEARCH_GIT_RAW)


    def do_calculate_developer_metrics_by_routine_class(owner_repo_group, table_name, class_name):
        if not owner_repo_group:
            logger.info("owner_repo组为空")
            return
        calc = MetricGroupRoutineCalculation(class_name,
                                             clickhouse_conn_info, mysql_conn_info,
                                             owner_repo_group, table_name)
        calc.routine()


    class_table_names = [
        (PrivilegeEventsMetricRoutineCalculation, "PrivilegeEventsMetricRoutineCalculation", "privilege_events"),
        (CountMetricRoutineCalculation, "CountMetricRoutineCalculation", "count_metrics"),
        (NetworkMetricRoutineCalculation, "NetworkMetricRoutineCalculation", "network_metrics"),
    ]
    for (class_, class_name, table_name) in class_table_names:
        op_calculate_developer_metrics = PythonOperator(
            task_id=f'op_do_{class_name}',
            python_callable=do_calculate_developer_metrics_by_routine_class,
            trigger_rule='all_done',
            op_kwargs={
                "owner_repo_group": uniq_owner_repos,
                "table_name": table_name,
                "class_name": class_
            }
        )
        # op_init_daily_gits_sync >> op_sync_gits_opensearch_group >> op_sync_gits_clickhouse_group
        # prev_op = op_sync_gits_clickhouse_group
