# -*-coding:utf-8-*-
# -*-coding:utf-8-*-
import datetime
import time
from oss_know.libs.util.log import logger

from oss_know.libs.util.clickhouse_driver import CKServer

insert_size = 40000


def get_email_tz_region_map(clickhouse_server_info):
    ck_client = CKServer(host=clickhouse_server_info["HOST"],
                         port=clickhouse_server_info["PORT"],
                         user=clickhouse_server_info["USER"],
                         password=clickhouse_server_info["PASSWD"],
                         database=clickhouse_server_info["DATABASE"])
    tz_0_sql = f"""
    ---统计0时区

select * from (select email, groupArray((region, commit_count)) as region_commit_count
from (select email, region, sum(commit_count) as commit_count
      from (select email,
                   tz,
                   multiIf(tz in (8), '中国', tz in (-1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12), '北美',
                           tz in (1, 2, 3, 4), '欧洲', tz in (5), '印度', tz in (10), '澳洲',
                           tz in (9), '日韩',
                           tz in (0), '0时区', '其他') as region,
                   sum(commit_count)               as commit_count
            from (select email, tz, count() as commit_count
                  from (select argMax(author_email, search_key__updated_at) as email,
                               argMax(author_tz, search_key__updated_at)    as tz
                        from gits
                        group by hexsha)
                  group by email, tz

                  union all

                  select email, tz, count() as commit_count
                  from (select argMax(committer_email, search_key__updated_at) as email,
                               argMax(committer_tz, search_key__updated_at)    as tz
                        from gits
                        group by hexsha)
                  group by email, tz)  where email global in (-- 有在0时区贡献的人
select email from (select email
from (select argMax(author_email, search_key__updated_at) as email,
             argMax(author_tz, search_key__updated_at)    as tz
      from gits
      group by hexsha)
where tz = 0
group by email

union all

select email
from (select argMax(committer_email, search_key__updated_at) as email,
             argMax(committer_tz, search_key__updated_at)    as tz
      from gits
      group by hexsha)
where tz = 0
group by email) group by email)
            group by email, tz
            order by email, commit_count desc)
      group by email, region
      order by email, commit_count desc)
group by email)
    """
    bulk_data = []
    update_at = datetime.datetime.now()
    # 13位时间戳
    update_at_timestamp = int(datetime.datetime.now().timestamp() * 1000)
    tz_0_sql_resp = ck_client.execute_no_params(tz_0_sql)
    for result in tz_0_sql_resp:
        email = result[0]
        timezones = result[1]
        if len(timezones) == 1:
            # 这种情况时区为0
            main_timezone = '0时区'
        else:
            if timezones[0][0] == '0时区':
                main_timezone = timezones[1][0]
            else:
                main_timezone = timezones[0][0]
        logger.info(
            f"email:{result[0]} timezones:{timezones} timezone_count:{len(timezones)} main_timezone:{main_timezone}")
        bulk_data.append({"update_at": update_at, "update_at_timestamp": update_at_timestamp, "email": email,
                          "main_tz_area": main_timezone, "top_n_tz_area": timezones})
        if len(bulk_data) == insert_size:
            resp = ck_client.execute("insert into table email_main_tz_map values", bulk_data)
            bulk_data.clear()
            logger.info(f"resp:{resp}")
    tz_not_0_sql = """
    ---统计非0时区

select * from (select email, groupArray((region, commit_count)) as region_commit_count
from (select email, region, sum(commit_count) as commit_count
      from (select email,
                   tz,
                   multiIf(tz in (8), '中国', tz in (-1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12), '北美',
                           tz in (1, 2, 3, 4), '欧洲', tz in (5), '印度', tz in (10), '澳洲',
                           tz in (9), '日韩',
                           tz in (0), '0时区', '其他') as region,
                   sum(commit_count)               as commit_count
            from (select email, tz, count() as commit_count
                  from (select argMax(author_email, search_key__updated_at) as email,
                               argMax(author_tz, search_key__updated_at)    as tz
                        from gits
                        group by hexsha)
                  group by email, tz

                  union all

                  select email, tz, count() as commit_count
                  from (select argMax(committer_email, search_key__updated_at) as email,
                               argMax(committer_tz, search_key__updated_at)    as tz
                        from gits
                        group by hexsha)
                  group by email, tz)  where email global not in (-- 有在0时区贡献的人
select email from (select email
from (select argMax(author_email, search_key__updated_at) as email,
             argMax(author_tz, search_key__updated_at)    as tz
      from gits
      group by hexsha)
where tz = 0
group by email

union all

select email
from (select argMax(committer_email, search_key__updated_at) as email,
             argMax(committer_tz, search_key__updated_at)    as tz
      from gits
      group by hexsha)
where tz = 0
group by email) group by email)
            group by email, tz
            order by email, commit_count desc)
      group by email, region
      order by email, commit_count desc)
group by email)

    """
    tz_not_0_sql_resp = ck_client.execute_no_params(tz_not_0_sql)
    for result in tz_not_0_sql_resp:
        email = result[0]
        timezones = result[1]
        # 直接取第一个时区
        main_timezone = timezones[0][0]
        logger.info(
            f"email:{email} timezones:{timezones} timezone_count:{len(timezones)} main_timezone:{main_timezone}")
        bulk_data.append({"update_at": update_at, "update_at_timestamp": update_at_timestamp, "email": email,
                          "main_tz_area": main_timezone, "top_n_tz_area": timezones})
        if len(bulk_data) == insert_size:
            resp = ck_client.execute("insert into table email_main_tz_map values", bulk_data)
            bulk_data.clear()
            logger.info(f"resp:{resp}")
    if bulk_data:
        resp = ck_client.execute("insert into table email_main_tz_map values", bulk_data)
        bulk_data.clear()
        logger.info(f"resp:{resp}")
