# -*-coding:utf-8-*-
from oss_know.libs.util.log import logger
from oss_know.libs.util.clickhouse_driver import CKServer


def get_github_id_approved_reviewed(clickhouse_server_info):
    ck_client = CKServer(host=clickhouse_server_info["HOST"],
                         port=clickhouse_server_info["PORT"],
                         user=clickhouse_server_info["USER"],
                         password=clickhouse_server_info["PASSWD"],
                         database=clickhouse_server_info["DATABASE"])
    sql = f"""
        insert into github_id_approved_reviewed_commented_map
    select now()                                                       as update_at,
           toUnixTimestamp(now())                                      as update_at_timestamp,
           search_key__owner,
           search_key__repo,
           toInt64(id),
           sum(JSONExtractString(timeline_raw, 'state') = 'approved')  as approved_count,
           sum(JSONExtractString(timeline_raw, 'state') = 'commented') as reviewed_commented
    from (select search_key__owner,
                 search_key__repo,
                 search_key__event,
                 search_key__number,
                 timeline_raw
          from github_issues_timeline
          where search_key__event = 'reviewed'
          group by search_key__owner, search_key__repo, search_key__event, search_key__number, timeline_raw)
    where id!=''
    group by search_key__owner, search_key__repo, JSONExtractString(JSONExtractString(timeline_raw, 'user'), 'id') as id
        """
    resp = ck_client.execute_no_params(sql)
    logger.info(f"resp:{resp}")
