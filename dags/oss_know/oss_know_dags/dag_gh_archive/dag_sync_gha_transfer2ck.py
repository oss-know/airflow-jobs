import datetime

from oss_know.libs.gh_archive import gh_archive

gha = gh_archive.GHArchive()

results = gha.get_hour(current_date=datetime.datetime(2022, 11, 27, 19))


def do_sync_transfer_gha_2ck(year_month_day_list):
    # from airflow.models import Variable
    # from oss_know.libs.gha.transfer_data_to_ck import parse_json_data
    for i in year_month_day_list:
        url = f"https://data.gharchive.org/{i}.json.gz"
        print(url)
        # gha.get_gh_archive_by_hour('/home/malin', url)
    # date_array = result.split('-')
    # year = date_array[0]
    # month = date_array[1]
    # day = date_array[2]
    # logger.info(f"year:{year}_month:{month}_day:{day}")
    # # 需要先下载
    # clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
    # parse_json_data(year, month, day, clickhouse_server_info=clickhouse_server_info)
    return "end do_transfer_gha_2ck"


for result in results:
    date_array = result.split('-')
    year = date_array[0]
    month = date_array[1]
    day = date_array[2]
    do_sync_transfer_gha_2ck(results[result])

    # for year in [2022]:
    #
    #
    #     for month in range(9, 10):
    #
    #         day_count = calendar.monthrange(year, month)[1]
    #         if month < 10:
    #             month = '0' + str(month)
    #         # for i in range(1, day_count + 1):
    #         for i in range(1, 2):
    #             if i < 10:
    #                 i = '0' + str(i)
    #             op_do_sync_transfer_gha_2ck = PythonOperator(
    #                 task_id=f'do_transfer_gha_2ck_year_{year}_month_{month}_day_{i}',
    #                 python_callable=do_sync_transfer_gha_2ck,
    #                 op_kwargs={'year': year, "month": month, 'day': i},
    #             )
    #             op_sync_gha_transfer2ck >> op_do_sync_transfer_gha_2ck
