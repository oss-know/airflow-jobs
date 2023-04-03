import calendar
from datetime import datetime
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA
from oss_know.libs.gh_archive.parse_data import parse_json_data

with DAG(
        dag_id='init_gh_archive_data',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['gh_archive'],
        concurrency=4
) as dag:
    def init_sync_gh_archive_data(ds, **kwargs):
        return 'Start init_sync_gh_archive_data'


    op_init_sync_gh_archive_data = PythonOperator(
        task_id='init_sync_git_info',
        python_callable=init_sync_gh_archive_data,
    )


    # def do_sync_gh_archive_data(year, month):
    #     from airflow.models import Variable
    #     from oss_know.libs.github import init_gits
    #     opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)
    #     print(year)
    #     print(month)
    #     day_count = calendar.monthrange(year, month)[1]
    #     if month < 10:
    #         month = '0'+str(month)
    #     for day in range(1, day_count+1):
    #         if day < 10:
    #             day = '0'+str(day)
    #         for hour in range(24):
    #             file_name = f"{year}-{month}-{day}-{hour}.json"
    #             parse_json_data(file_name,opensearch_conn_datas)
    #
    #     return 'do_sync_git_info:::end'

    def do_sync_gh_archive_data(year, month, day):
        from airflow.models import Variable
        from oss_know.libs.github import init_gits
        opensearch_conn_datas = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
        print(year)
        print(month)
        print(day)
        for hour in range(24):
            file_name = f'{year}-{month}-{day}-{hour}.json'
            parse_json_data(file_name=file_name, opensearch_conn_infos=opensearch_conn_datas)

        return 'do_sync_git_info:::end'


    from airflow.models import Variable

    # years = Variable.get("gh_archive_data_years", deserialize_json=True)

    # for year in years:
    #     for i in range(1, 13):
    #         month = i
    #         op_do_init_sync_gh_archive_data = PythonOperator(
    #             task_id=f'do_sync_gh_archive_data_year_{year}_month_{month}',
    #             python_callable=do_sync_gh_archive_data,
    #             op_kwargs={'year': year,"month":month},
    #         )
    #
    #     op_init_sync_gh_archive_data >> op_do_init_sync_gh_archive_data

    for year in range(2019, 2020):

        for month in range(2, 3):
            day_count = calendar.monthrange(year, month)[1]
            if month < 10:
                month = '0' + str(month)
            for i in range(1, day_count + 1):
                if i < 10:
                    i = '0' + str(i)
                op_do_init_sync_gh_archive_data = PythonOperator(
                    task_id=f'do_sync_gh_archive_data_year_{year}_month_{month}_day_{i}',
                    python_callable=do_sync_gh_archive_data,
                    op_kwargs={'year': year, "month": month, 'day': i},
                )

                op_init_sync_gh_archive_data >> op_do_init_sync_gh_archive_data
