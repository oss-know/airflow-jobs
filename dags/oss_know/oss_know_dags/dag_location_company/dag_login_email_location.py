# -*-coding:utf-8-*-
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# clickhouse_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO, LOCATION_COMPANY_RUN_CONFIG
from oss_know.libs.location_company.location_company import LoginEmailLocationCompany

with DAG(
        dag_id='init_login_email_location',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['location_company'],
        concurrency=6
) as dag:

    from airflow.models import Variable

    clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
    lelc = LoginEmailLocationCompany(clickhouse_server_info)


    def do_init():
        return "end:::do_init"


    def do_get_github_id_tz_region_map():
        lelc.get_github_id_tz_region_map()

        return "end:::do_get_github_id_tz_region_map"


    def do_get_github_login_location():
        lelc.get_github_login_location()
        return "end:::do_get_github_login_location"


    def do_get_email_location():
        lelc.get_email_location()
        return "end:::do_get_email_location"


    def do_code_owner_location(owner, repo):
        lelc.code_owner_location(owner, repo)
        return "end:::do_code_owner_location"


    def do_get_login_company_time_range():
        lelc.get_login_company_time_range()
        return "end:::do_get_login_company_time_range"


    def do_company_email_domain_map():
        lelc.company_email_domain_map()
        return "end:::do_company_email_domain_map"


    def do_get_email_company_range():
        lelc.get_email_company_range()
        return "end:::do_get_email_company_range"


    def do_code_owner_company(owner, repo):
        lelc.code_owner_company(owner, repo)
        return "end:::do_code_owner_company"


    # 添加配置文件 控制需要运行时间长的任务反复运行


    op_do_init = PythonOperator(
        task_id='do_init',
        python_callable=do_init,
    )

    from airflow.models import Variable

    location_company_run_config = Variable.get(LOCATION_COMPANY_RUN_CONFIG, deserialize_json=True)


    def handle_location_operator(config):
        operators = [op_do_init]

        if config.get('force_run_do_get_github_id_tz_region_map'):
            op_do_get_github_id_tz_region_map = PythonOperator(
                task_id=f'do_get_github_id_tz_region_map',
                python_callable=do_get_github_id_tz_region_map,
            )
            op_do_get_github_login_location = PythonOperator(
                task_id='do_get_github_login_location',
                python_callable=do_get_github_login_location,
            )

            op_do_get_email_location = PythonOperator(
                task_id='do_get_email_location',
                python_callable=do_get_email_location,
            )

            operators.extend(
                [op_do_get_github_id_tz_region_map])
            operators.extend(
                [op_do_get_github_login_location, op_do_get_email_location])
        else:

            if config.get('force_run_do_get_github_login_location'):
                op_do_get_github_login_location = PythonOperator(
                    task_id='do_get_github_login_location',
                    python_callable=do_get_github_login_location,
                )

                op_do_get_email_location = PythonOperator(
                    task_id='do_get_email_location',
                    python_callable=do_get_email_location,
                )

                operators.extend(
                    [op_do_get_github_login_location, op_do_get_email_location])

        if location_company_run_config.get('force_run_do_code_owner_company'):
            repos = location_company_run_config.get('need_infer_team_member_location_repos')
            code_owner_operators = []
            for owner_repo in repos:
                op_do_code_owner_location = PythonOperator(
                    task_id=f'do_code_owner_{owner_repo["owner"]}_repo_{owner_repo["repo"]}_location',
                    python_callable=do_code_owner_location,
                    op_kwargs={
                        'owner': owner_repo['owner'],
                        'repo': owner_repo['repo'],
                    }
                )
                code_owner_operators.append(op_do_code_owner_location)
            operators.append(tuple(code_owner_operators))
        return operators


    def handle_company_operator(config):
        operators = [op_do_init]
        op_do_company_email_domain_map = None
        if location_company_run_config.get('force_run_do_company_email_domain_map'):
            op_do_company_email_domain_map = PythonOperator(
                task_id='do_company_email_domain_map',
                python_callable=do_company_email_domain_map,
            )
        if config.get('force_run_do_get_login_company_time_range'):
            op_do_get_login_company_time_range = PythonOperator(
                task_id='do_get_login_company_time_range',
                python_callable=do_get_login_company_time_range,
            )
            op_do_get_email_company_range = PythonOperator(
                task_id='do_get_email_company_range',
                python_callable=do_get_email_company_range,
            )
            if op_do_company_email_domain_map:
                operators.append((op_do_get_login_company_time_range, op_do_company_email_domain_map))
            else:
                operators.append(op_do_get_login_company_time_range)
                operators.append(op_do_get_email_company_range)

        else:
            if op_do_company_email_domain_map:
                operators.extend([op_do_company_email_domain_map])

        if location_company_run_config.get('force_run_do_code_owner_company'):
            repos = location_company_run_config.get('need_infer_team_member_company_repos')
            code_owner_operators = []

            for owner_repo in repos:
                op_do_code_owner_company = PythonOperator(
                    task_id=f'do_code_owner_{owner_repo["owner"]}_repo_{owner_repo["repo"]}_company',
                    python_callable=do_code_owner_company,
                    op_kwargs={
                        'owner': owner_repo['owner'],
                        'repo': owner_repo['repo'],
                    }
                )
                code_owner_operators.append(op_do_code_owner_company)
            operators.append(tuple(code_owner_operators))
        return operators

    # task 有前后依赖关系  可在配置文件中配置暂不需要运行的步骤
    location_operators = handle_location_operator(location_company_run_config)
    company_operators = handle_company_operator(location_company_run_config)

    for i in range(1, len(location_operators)):
        location_operators[i - 1] >> location_operators[i]

    for i in range(1, len(company_operators)):
        company_operators[i - 1] >> company_operators[i]
