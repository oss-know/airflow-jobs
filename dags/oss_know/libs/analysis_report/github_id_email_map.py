# -*-coding:utf-8-*-
# -*-coding:utf-8-*-
import datetime
import time
from oss_know.libs.util.log import logger

from oss_know.libs.util.clickhouse_driver import CKServer

insert_size = 40000


def get_github_id_email_map(clickhouse_server_info):
    ck_client = CKServer(host=clickhouse_server_info["HOST"],
                         port=clickhouse_server_info["PORT"],
                         user=clickhouse_server_info["USER"],
                         password=clickhouse_server_info["PASSWD"],
                         database=clickhouse_server_info["DATABASE"])
    ck_client.execute_no_params(""" insert into table github_id_email_map
                                    select now(), toUnixTimestamp(now()), github_id, email 
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
                                    group by sha) group by email,github_id""")
