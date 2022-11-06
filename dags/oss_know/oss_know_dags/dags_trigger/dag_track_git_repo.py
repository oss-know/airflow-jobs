from datetime import datetime
from string import Template

import pandas as pd
import psycopg2
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, GITHUB_TOKENS, PROXY_CONFS, \
    CLICKHOUSE_DRIVER_INFO, CK_TABLE_DEFAULT_VAL_TPLT, POSTGRES_CONN_INFO
# git_init_sync_v0.0.3
from oss_know.libs.clickhouse import init_ck_transfer_data
from oss_know.libs.metrics.init_analysis_data_for_dashboard import get_alter_files_count, \
    get_contributer_by_dir_email_domain, get_dir_contributer_count, get_tz_distribution, \
    get_alter_file_count_by_dir_email_domain, get_dir_n
from oss_know.libs.util.log import logger
from oss_know.libs.util.proxy import KuaiProxyService, ProxyManager, GithubTokenProxyAccommodator
from oss_know.libs.util.token import TokenManager

# The essential OpenSearch indices and ClickHouse tables name mapping
# The github_issues_timeline is handled separately
OS_CK_MAPPING = [
    {
        "CK_TABLE_NAME": "gits",
        "OPENSEARCH_INDEX": "gits"
    },
    {
        "CK_TABLE_NAME": "github_commits",
        "OPENSEARCH_INDEX": "github_commits"
    },
    {
        "CK_TABLE_NAME": "github_pull_requests",
        "OPENSEARCH_INDEX": "github_pull_requests"
    },
    {
        "CK_TABLE_NAME": "github_issues",
        "OPENSEARCH_INDEX": "github_issues"
    },
    {
        "CK_TABLE_NAME": "github_issues_comments",
        "OPENSEARCH_INDEX": "github_issues_comments"
    }
]

with DAG(
        dag_id='git_track_repo',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def init_track_repo():
        return 'Start init_track_repo'


    op_init_track_repo = PythonOperator(
        task_id='init_track_repo',
        python_callable=init_track_repo,
    )

    git_save_local_path = Variable.get("git_save_local_path", deserialize_json=True)
    github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
    opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
    clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)

    proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
    proxies = [f'http://{line}' for line in proxy_confs['reserved_proxies']]
    proxy_service = KuaiProxyService(proxy_confs['api_url'], proxy_confs['orderid'])
    proxy_manager = ProxyManager(proxies, proxy_service)
    token_manager = TokenManager(github_tokens)
    proxy_accommodator = GithubTokenProxyAccommodator(token_manager, proxy_manager, shuffle=True,
                                                      policy=GithubTokenProxyAccommodator.POLICY_FIXED_MAP)

    postgres_conn_info = Variable.get(POSTGRES_CONN_INFO, deserialize_json=True)
    pg_conn = psycopg2.connect(host=postgres_conn_info["host"], port=postgres_conn_info['port'],
                               database=postgres_conn_info['database'],
                               user=postgres_conn_info['user'],
                               password=postgres_conn_info['password'])
    pg_cur = pg_conn.cursor()


    def do_init_gits(callback, **kwargs):
        from oss_know.libs.github import init_gits
        owner = kwargs['dag_run'].conf.get('owner')
        repo = kwargs['dag_run'].conf.get('repo')
        url = kwargs['dag_run'].conf.get('url')
        gits_args = [url, owner, repo, None, opensearch_conn_info, git_save_local_path]
        exec_job_and_update_db(init_gits.init_sync_git_datas, callback, 'gits', args=gits_args, **kwargs)


    def do_init_github_commits(callback, **kwargs):
        from oss_know.libs.github import init_commits
        until = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        additional_args = [until]
        exec_job_and_update_db(init_commits.init_github_commits, callback, 'github_commits', additional_args, **kwargs)


    def do_init_github_issues(callback, **kwargs):
        from oss_know.libs.github import init_issues
        exec_job_and_update_db(init_issues.init_github_issues, callback, 'github_issues', **kwargs)


    def do_init_github_issues_comments(callback, **kwargs):
        from oss_know.libs.github import init_issues_comments
        exec_job_and_update_db(init_issues_comments.init_github_issues_comments, callback, 'github_issues_comments',
                               **kwargs)


    def do_init_github_issues_timeline(callback, **kwargs):
        from oss_know.libs.github import init_issues_timeline
        exec_job_and_update_db(init_issues_timeline.init_sync_github_issues_timeline, callback,
                               'github_issues_timeline', **kwargs)


    def do_init_github_pull_requests(callback, **kwargs):
        from oss_know.libs.github import init_pull_requests
        exec_job_and_update_db(init_pull_requests.init_sync_github_pull_requests, callback,
                               'github_pull_requests', **kwargs)


    def do_ck_transfer(callback, **kwargs):
        owner = kwargs['dag_run'].conf.get('owner')
        repo = kwargs['dag_run'].conf.get('repo')
        dag_run_id = kwargs['dag_run'].conf.get('dag_run_id')
        search_key = {"owner": owner, "repo": repo}
        table_templates = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)

        check_status_sql = \
            f"""select ck_transfer_status
            from triggered_git_repos
            where owner = '{owner}'
              and repo = '{repo}'
              and dag_run_id='{dag_run_id}';
            """

        pg_cur.execute(check_status_sql)
        pg_conn.commit()
        check_result = pg_cur.fetchall()
        logger.info(f'checking last job status with sql:\n{check_status_sql}\n{check_result}')
        if check_result and check_result[0][0] == 2:
            logger.info(f'{owner}/{repo}/{dag_run_id} ck_transfer status is success, skip')
            callback(owner, repo, dag_run_id, 'ck_transfer', None)
            return

        update_status_template_str = \
            f"""update triggered_git_repos 
                    set ck_transfer_status=$status 
                    where owner='{owner}' and repo='{repo}' and dag_run_id='{dag_run_id}'"""
        update_status_template = Template(update_status_template_str)

        try:
            # TODO The status update in pg(including the template above) is yanked from exec_job_and_update_db
            # Unify the code in later releases
            # To do this, exec_job_and_update_db should be updated to support batch jobs execution, since
            # ck_transfer include multiple jobs while other git/github data fetchings are all single-job
            pg_cur.execute(update_status_template.substitute(status=1))
            pg_conn.commit()

            # Handle timeline separately
            init_ck_transfer_data.transfer_data_special_by_repo(
                clickhouse_server_info=clickhouse_server_info,
                opensearch_index='github_issues_timeline',
                table_name='github_issues_timeline',
                opensearch_conn_datas=opensearch_conn_info, search_key=search_key)

            # Transfer all other git/github data
            # TODO We should support other git services like gitlab, gitee in the future
            for os_ck_map in OS_CK_MAPPING:
                os_index_name = os_ck_map['OPENSEARCH_INDEX']
                ck_table_name = os_ck_map['CK_TABLE_NAME']

                template = None
                for table_template in table_templates:
                    if table_template.get("table_name") == ck_table_name:
                        template = table_template.get("temp")
                        break
                df = pd.json_normalize(template)
                template = init_ck_transfer_data.parse_data_init(df)

                init_ck_transfer_data.transfer_data_by_repo(
                    clickhouse_server_info=clickhouse_server_info,
                    opensearch_index=os_index_name,
                    table_name=ck_table_name,
                    opensearch_conn_datas=opensearch_conn_info,
                    template=template, search_key=search_key, transfer_type='github_git_init_by_repo')

            pg_cur.execute(update_status_template.substitute(status=2))
            pg_conn.commit()
            callback(owner, repo, dag_run_id, 'ck_transfer', None)
        except Exception as e:
            pg_cur.execute(update_status_template.substitute(status=3))
            pg_conn.commit()
            callback(owner, repo, dag_run_id, 'ck_transfer', e)


    def do_ck_aggregation(callback, **kwargs):
        owner = kwargs['dag_run'].conf.get('owner')
        repo = kwargs['dag_run'].conf.get('repo')
        dag_run_id = kwargs['dag_run'].conf.get('dag_run_id')

        update_status_template_str = \
            f"""update triggered_git_repos 
                            set ck_aggregation_status=$status 
                            where owner='{owner}' and repo='{repo}' and dag_run_id='{dag_run_id}'"""
        update_status_template = Template(update_status_template_str)

        try:
            # get_dir_n must first run, since other aggregations depend on it.
            get_dir_n(ck_con=clickhouse_server_info, owner=owner, repo=repo)

            get_alter_files_count(ck_con=clickhouse_server_info, owner=owner, repo=repo)
            get_dir_contributer_count(ck_con=clickhouse_server_info, owner=owner, repo=repo)
            get_alter_file_count_by_dir_email_domain(ck_con=clickhouse_server_info, owner=owner, repo=repo)
            get_contributer_by_dir_email_domain(ck_con=clickhouse_server_info, owner=owner, repo=repo)
            get_tz_distribution(ck_con=clickhouse_server_info, owner=owner, repo=repo)

            pg_cur.execute(update_status_template.substitute(status=2))
            pg_conn.commit()
            callback(owner, repo, dag_run_id, 'ck_aggregation', None)
        except Exception as e:
            pg_cur.execute(update_status_template.substitute(status=3))
            pg_conn.commit()
            callback(owner, repo, dag_run_id, 'ck_aggregation', e)


    def exec_job_and_update_db(job_callable, callback, res_type, additional_args=[], args=[], **kwargs):
        owner = None
        repo = None
        url = None
        dag_run_id = None
        try:
            owner = kwargs['dag_run'].conf.get('owner')
            repo = kwargs['dag_run'].conf.get('repo')
            dag_run_id = kwargs['dag_run'].conf.get('dag_run_id')
            url = kwargs['dag_run'].conf.get('url')
        except KeyError:
            logger.error(f'Repo info not found in dag run config, init dag from Variable')

        check_status_sql = \
            f"""select {res_type}_status
            from triggered_git_repos
            where owner = '{owner}'
            and repo = '{repo}'
            and dag_run_id='{dag_run_id}'"""
        pg_cur.execute(check_status_sql)
        pg_conn.commit()
        check_result = pg_cur.fetchall()
        logger.info(f'checking last job status with sql:\n{check_status_sql}\n{check_result}')
        if check_result and check_result[0][0] == 2:
            logger.info(f'{owner}/{repo} {res_type} / {dag_run_id} status is success, skip')
            callback(owner, repo, dag_run_id, res_type, None)
            return

        update_status_template_str = \
            f"""update triggered_git_repos 
            set {res_type}_status=$status
            where owner='{owner}' and repo='{repo}' and dag_run_id='{dag_run_id}'"""
        update_status_template = Template(update_status_template_str)

        if 'github.com' not in url:
            logger.info('Not github repository, skip')
            pg_cur.execute(update_status_template.substitute(status=2))
            pg_conn.commit()
            callback(owner, repo, dag_run_id, res_type, None)
            return

        since = '1970-01-01T00:00:00Z'
        try:
            pg_cur.execute(update_status_template.substitute(status=1))
            pg_conn.commit()
            if args:
                job_callable(*args)
            else:
                job_args = [opensearch_conn_info, owner, repo, proxy_accommodator, since] + additional_args
                job_callable(*job_args)
            pg_cur.execute(update_status_template.substitute(status=2))
            pg_conn.commit()

            callback(owner, repo, dag_run_id, res_type, None)
        except Exception as e:
            pg_cur.execute(update_status_template.substitute(status=3))
            pg_conn.commit()
            callback(owner, repo, dag_run_id, res_type, e)


    def job_callback(owner, repo, dag_run_id, res_type, e: Exception):
        logger.info(f'Finish job on {owner}/{repo} {res_type} {dag_run_id}, exception:', e)
        job_status = 'failed' if e else 'success'
        update_status_sql = f"""update triggered_git_repos 
                        set job_status=%s 
                        where owner=%s and repo=%s and dag_run_id=%s"""

        if e:
            update_fail_reason_sql = f"""update triggered_git_repos 
                        set {res_type}_fail_reason = %s
                        where owner=%s and repo=%s and dag_run_id=%s"""
            pg_cur.execute(update_status_sql, (job_status, owner, repo, dag_run_id))
            pg_cur.execute(update_fail_reason_sql, (str(e), owner, repo, dag_run_id))
            pg_conn.commit()
            raise e

        get_statuses_sql = f"""
        select gits_status,
               github_commits_status,
               github_pull_requests_status,
               github_issues_status,
               github_issues_comments_status,
               github_issues_timeline_status,
               ck_transfer_status
               ck_aggregation_status
        from triggered_git_repos
        where owner='{owner}' and repo='{repo}' and dag_run_id='{dag_run_id}'"""
        pg_cur.execute(get_statuses_sql)
        records = pg_cur.fetchall()
        if not records:  # or records' length is not 1
            raise Exception(f'Failed to check {owner}/{repo} job status, record not found')

        job_finished = True
        for status in records[0]:
            if status != 2:
                job_finished = False
        if job_finished:
            pg_cur.execute(update_status_sql, (job_status, owner, repo, dag_run_id))
            pg_conn.commit()


    op_do_init_gits = PythonOperator(
        task_id=f'do_init_gits',
        python_callable=do_init_gits,
        provide_context=True,
        op_kwargs={'callback': job_callback}
    )

    op_do_init_github_commits = PythonOperator(
        task_id=f'do_init_github_commits',
        python_callable=do_init_github_commits,
        provide_context=True,
        op_kwargs={'callback': job_callback}
    )

    op_do_init_github_issues = PythonOperator(
        task_id=f'do_init_github_issues',
        python_callable=do_init_github_issues,
        provide_context=True,
        op_kwargs={'callback': job_callback}
    )

    op_do_init_github_issues_comments = PythonOperator(
        task_id=f'do_init_github_issues_comments',
        python_callable=do_init_github_issues_comments,
        provide_context=True,
        op_kwargs={'callback': job_callback}
    )

    op_do_init_github_issues_timeline = PythonOperator(
        task_id=f'do_init_github_issues_timeline',
        python_callable=do_init_github_issues_timeline,
        provide_context=True,
        op_kwargs={'callback': job_callback}
    )

    op_do_init_github_pull_requests = PythonOperator(
        task_id=f'do_init_github_pull_requests',
        python_callable=do_init_github_pull_requests,
        provide_context=True,
        op_kwargs={'callback': job_callback}
    )

    op_do_ck_transfer = PythonOperator(
        task_id=f'do_ck_transfer',
        python_callable=do_ck_transfer,
        provide_context=True,
        op_kwargs={'callback': job_callback}
    )

    op_do_ck_aggregation = PythonOperator(
        task_id='do_ck_aggregation',
        python_callable=do_ck_aggregation,
        provide_context=True,
        op_kwargs={'callback': job_callback}
    )

    op_init_track_repo >> [op_do_init_gits, op_do_init_github_commits, op_do_init_github_pull_requests,
                           op_do_init_github_issues]
    op_do_init_github_issues >> [op_do_init_github_issues_comments,
                                 op_do_init_github_issues_timeline]
    [op_do_init_gits, op_do_init_github_commits, op_do_init_github_pull_requests,
     op_do_init_github_issues_comments, op_do_init_github_issues_timeline] >> op_do_ck_transfer
    op_do_ck_transfer >> op_do_ck_aggregation
