# -*-coding:utf-8-*-
# -*-coding:utf-8-*-
import datetime
import time
from oss_know.libs.util.log import logger

from oss_know.libs.util.clickhouse_driver import CKServer

insert_size = 40000


def get_github_id_tz_region_map(clickhouse_server_info):
    ck_client = CKServer(host=clickhouse_server_info["HOST"],
                              port=clickhouse_server_info["PORT"],
                              user=clickhouse_server_info["USER"],
                              password=clickhouse_server_info["PASSWD"],
                              database=clickhouse_server_info["DATABASE"])
    id_tz_region_sql = f"""
                select a.*,area from (select a.*,b.location from (select github_id,groupArray((emails,region,commit_count)) as tz_area_map
        from (select github_id,
                     region,
                     groupArray(email) as emails,
                     sum(commit_count) as commit_count
              from (select email, region, sum(commit_count) as commit_count
                    from (select email,
                                 tz,
                                 multiIf(tz in (8), '中国',
                                         tz in (-1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12),
                                         '北美',
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
                                group by email, tz)
                          group by email, tz
                          order by email, commit_count desc)
                    group by email, region
                    order by email, commit_count desc) as a global join (select email, github_id
                                  from (select argMax(commit__author__email, search_key__updated_at) as email,
                                               argMax(author__id, search_key__updated_at)            as github_id
                                        from github_commits
                                        where author__id != 0
                                        group by sha
                                        union all
                                        select argMax(commit__committer__email, search_key__updated_at) as email,
                                               argMax(committer__id, search_key__updated_at)            as github_id
                                        from github_commits
                                        where committer__id != 0
                                        group by sha
                                           )
                                  group by email, github_id) as b on a.email = b.email
              where github_id != 0
              group by github_id, region
              order by github_id, commit_count desc)
        group by github_id) as a global left join (select id, argMax(country_inferred_from_location,updated_at) as location from github_profile where country_inferred_from_location!= ''group by id) as b
    on a.github_id = b.id) as a global left join (select country_or_region,argMax(area,update_at) area from country_tz_region_map group by country_or_region
    ) as b on a.location = b.country_or_region
                """
    bulk_data = []
    update_at = datetime.datetime.now()
    # 13位时间戳
    update_at_timestamp = int(datetime.datetime.now().timestamp() * 1000)
    id_tz_region_resp = ck_client.execute_no_params(id_tz_region_sql)
    for result in id_tz_region_resp:
        github_id = result[0]
        timezones = result[1]
        area = result[3]
        location = result[2]
        main_timezone = ''
        if timezones[0][1] == '0时区' and len(timezones) > 1:
            main_timezone = timezones[1][1]
        elif timezones[0][1] == '0时区' and len(timezones) == 1:
            main_timezone = '0时区'
        else:
            main_timezone = timezones[0][1]
        inferred_area = main_timezone
        if area:
            inferred_area = area
        logger.info(
            f"github_id:{result[0]} timezones:{timezones} timezone_count:{len(timezones)} main_timezone:{main_timezone} location:{location}")
        bulk_data.append({"update_at": update_at, "update_at_timestamp": update_at_timestamp, "github_id": github_id,
                          "main_tz_area": main_timezone, "inferred_area": inferred_area, "location": location,
                          "top_n_tz_area": timezones})
        if len(bulk_data) == insert_size:
            resp = ck_client.execute("insert into table github_id_main_tz_map values", bulk_data)
            bulk_data.clear()
            logger.info(f"resp:{resp}")
    #
    if bulk_data:
        resp = ck_client.execute("insert into table github_id_main_tz_map values", bulk_data)
        bulk_data.clear()
        logger.info(f"resp:{resp}")
