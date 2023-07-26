from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_GIT_RAW
from oss_know.libs.util.clickhouse import get_uniq_owner_repos
from oss_know.libs.util.log import logger
from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO, DAILY_SYNC_INTERVAL, \
    MYSQL_CONN_INFO, ROUTINELY_UPDATE_SAMPLE_INFLUENCE_METRICS_INTERVAL
from oss_know.libs.metrics.influence_metrics import calculate_sample_influence_metrics, save_sample_influence_metrics, \
    TotalFixIndensityMetricRoutineCalculation, MetricGroupRoutineCalculation, DeveloperRoleMetricRoutineCalculation, \
    ContributedRepoFirstYearMetricRoutineCalculation, BasicContributorGraphMetricRoutineCalculation, \
    AverageRepairOfPeersMetricRoutineCalculation
from oss_know.libs.util.base import arrange_owner_repo_into_letter_groups

# sync_interval = Variable.get(ROUTINELY_UPDATE_SAMPLE_INFLUENCE_METRICS_INTERVAL, default_var=None)
# if not sync_interval:
#     sync_interval = Variable.get(DAILY_SYNC_INTERVAL, default_var=None)

mysql_conn_info = Variable.get(MYSQL_CONN_INFO, deserialize_json=True)

with DAG(dag_id='routinely_calculate_sample_influence_metrics',  # schedule_interval='*/5 * * * *',
         schedule_interval=None,
         start_date=datetime(2021, 1, 1), catchup=False,
         tags=['metrics'], ) as dag:
    uniq_owner_repos = Variable.get(ROUTINELY_UPDATE_SAMPLE_INFLUENCE_METRICS_INTERVAL, deserialize_json=True)
    clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)

    if not uniq_owner_repos:
        # 如果没有在variable中指定需要计算metrics的项目则从库中获取全量的去重后的owner_repo 计算
        clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        uniq_owner_repos = get_uniq_owner_repos(clickhouse_conn_info, OPENSEARCH_GIT_RAW)

    task_groups_by_capital_letter = arrange_owner_repo_into_letter_groups(uniq_owner_repos)


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


    def do_calculate_sample_influence_metrics_by_routine_class(owner_repo_group):
        if not owner_repo_group:
            logger.info("owner_repo组为空")
            return
        class_table_names = [
            (TotalFixIndensityMetricRoutineCalculation, "total_fix_intensity"),
            (DeveloperRoleMetricRoutineCalculation,"developer_roles_metrics"),
            (BasicContributorGraphMetricRoutineCalculation,"contribution_graph_data"),
            (ContributedRepoFirstYearMetricRoutineCalculation,"contributed_repos_role"),
            (AverageRepairOfPeersMetricRoutineCalculation, "peers_average_fix_intensity_role")
        ]

        for class_tbname in class_table_names:
            class_name = class_tbname[0]
            table_name = class_tbname[1]
            calc = MetricGroupRoutineCalculation(class_name,
                                                 clickhouse_conn_info, mysql_conn_info,
                                                 owner_repo_group, table_name)
            calc.routine()


    # TODO Currently the DAG makes 27 parallel task groups(serial execution inside each group)
    #  Check in production env if it works as expected(won't make too much pressure on opensearch and
    #  clickhouse. Another approach is to make all groups serial, one after another, which assign
    #  init_op to prev_op at the beginning, then assign op_sync_gits_clickhouse_group to prev_op
    #  in each loop iteration(as commented below)
    #  Another tip: though the groups dict is by default sorted by alphabet, the generated DAG won't
    #  respect the order
    # prev_op = op_init_daily_gits_sync
    for letter, owner_repos in task_groups_by_capital_letter.items():
        op_calculate_sample_influence_metrics = PythonOperator(
            task_id=f'op_calculate_sample_influence_metrics_{letter}',
            python_callable=do_calculate_sample_influence_metrics_by_routine_class,
            trigger_rule='all_done',
            op_kwargs={
                "owner_repo_group": owner_repos
            }
        )
        # op_init_daily_gits_sync >> op_sync_gits_opensearch_group >> op_sync_gits_clickhouse_group
        # prev_op = op_sync_gits_clickhouse_group
