# -*-coding:utf-8-*-
import csv
import os
import socket
from datetime import datetime

from oss_know.libs.util.clickhouse_driver import CKServer


def get_data_csv(department, ck_conn_info):
    ck = CKServer(host=ck_conn_info["HOST"],
                  port=ck_conn_info["PORT"],
                  user=ck_conn_info["USER"],
                  password=ck_conn_info["PASSWD"],
                  database=ck_conn_info["DATABASE"])
    get_data_sql = f"""select department,
                      owner,
                      repo,
                      dir_level2,
                      domain_distribution,
                      tz_distribution,
                      email,
                      sum_tz_commit_files_count,
                      tz_commit_files_count,
                      profile_location,
                      profile_company,
                      inferred_from_location_country_or_region,
                      inferred_from_location_locality_or_region
                from dir2_analysis_v1
                where department = '{department}'
            """
    results = ck.execute_no_params(get_data_sql)
    with open(f"/opt/airflow/csv/二级目录统计-{department}-{datetime.now().strftime('%Y-%m-%dT%H-%M-%SZ')}.csv", "w", encoding='utf8',
              newline='') as csvfile:
        writer = csv.writer(csvfile)

        # 先写入columns_name
        writer.writerow(
            ["department",
             "owner",
             "repo",
             "dir_level2",
             "邮箱domain分布",
             "2级目录提交时区分布",
             "email",
             "个人文件提交数",
             "个人提交时区分布",
             "location",
             "company",
             "inferred_country_or_region",
             "inferred_city_or_region"
             ])
        for result in results:
            writer.writerow(list(result))
    # print(os.popen('whoami').readlines(), '-----------------------------------')
    ck.close()