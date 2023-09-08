from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_GIT_RAW
from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO, MYSQL_CONN_INFO, \
    ROUTINELY_UPDATE_INFLUENCE_METRICS_INCLUDES, ROUTINELY_UPDATE_INFLUENCE_METRICS_INTERVAL
from oss_know.libs.metrics.influence_metrics import calculate_sample_influence_metrics, \
    save_sample_influence_metrics, TotalFixIndensityMetricRoutineCalculation, \
    MetricGroupRoutineCalculation, DeveloperRoleMetricRoutineCalculation, \
    ContributedRepoFirstYearMetricRoutineCalculation, \
    BasicContributorGraphMetricRoutineCalculation, AverageRepairOfPeersMetricRoutineCalculation
from oss_know.libs.util.clickhouse import get_uniq_owner_repos
from oss_know.libs.util.log import logger

sync_interval = Variable.get(ROUTINELY_UPDATE_INFLUENCE_METRICS_INTERVAL, default_var=None)
mysql_conn_info = Variable.get(MYSQL_CONN_INFO, deserialize_json=True)

with DAG(dag_id='routinely_calculate_contribution_metrics',  # schedule_interval='*/5 * * * *',
         schedule_interval=None,
         start_date=datetime(2021, 1, 1), catchup=False,
         tags=['metrics'], ) as dag:
    uniq_owner_repos = Variable.get(ROUTINELY_UPDATE_INFLUENCE_METRICS_INCLUDES, deserialize_json=True,
                                    default_var=None)
    clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)

    if not uniq_owner_repos:
        # 如果没有在variable中指定需要计算metrics的项目则从库中获取全量的去重后的owner_repo 计算
        clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        uniq_owner_repos = get_uniq_owner_repos(clickhouse_conn_info, OPENSEARCH_GIT_RAW)


    def do_calculate_sample_influence_metrics(owner_repo_group):
        batch = []
        batch_size = 5000
        for (owner, repo) in owner_repo_group:
            metrics = calculate_sample_influence_metrics(owner, repo)
            batch += metrics

            if len(batch) >= batch_size:
                save_sample_influence_metrics(mysql_conn_info, metrics)
                batch = []

        if batch:
            save_sample_influence_metrics(mysql_conn_info, metrics)


    def do_calculate_influence_metrics_by_routine_class(owner_repo_group, table_name, class_name):
        if not owner_repo_group:
            logger.info("owner_repo组为空")
            return
        calc = MetricGroupRoutineCalculation(class_name,
                                             clickhouse_conn_info, mysql_conn_info,
                                             owner_repo_group, table_name)
        calc.routine()


    class_table_names = [
        (TotalFixIndensityMetricRoutineCalculation, "TotalFixIndensityMetricRoutineCalculation", "total_fix_intensity"),
        (DeveloperRoleMetricRoutineCalculation, "DeveloperRoleMetricRoutineCalculation", "developer_roles_metrics"),
        (BasicContributorGraphMetricRoutineCalculation, "BasicContributorGraphMetricRoutineCalculation",
         "contribution_graph_data"),
        (ContributedRepoFirstYearMetricRoutineCalculation, "ContributedRepoFirstYearMetricRoutineCalculation",
         "contributed_repos_role"),
        (AverageRepairOfPeersMetricRoutineCalculation, "AverageRepairOfPeersMetricRoutineCalculation",
         "peers_average_fix_intensity_role")
    ]
    for (class_, class_name, table_name) in class_table_names:
        op_calculate_influence_metrics = PythonOperator(
            task_id=f'op_do_{class_name}',
            python_callable=do_calculate_influence_metrics_by_routine_class,
            trigger_rule='all_done',
            op_kwargs={
                "owner_repo_group": uniq_owner_repos,
                "table_name": table_name,
                "class_name": class_
            }
        )
        # op_init_daily_gits_sync >> op_sync_gits_opensearch_group >> op_sync_gits_clickhouse_group
        # prev_op = op_sync_gits_clickhouse_group
