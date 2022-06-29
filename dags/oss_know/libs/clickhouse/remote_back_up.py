# -*-coding:utf-8-*-
from loguru import logger
from oss_know.libs.util.clickhouse_driver import CKServer


def back_up_from_remote_server(clickhouse_server_info, params):
    ck_host = clickhouse_server_info["HOST"]
    ck_port = clickhouse_server_info["PORT"]
    ck_database = clickhouse_server_info["DATABASE"]
    ck_user = clickhouse_server_info["USER"]
    ck_password = clickhouse_server_info["PASSWD"]
    ck = CKServer(host='192.168.8.2', port=19000, database='default', user='default', password='default')
    remote_address = params['remote_address']
    remote_table_name = params['remote_table_name']
    remote_user = params['remote_user']
    remote_password = params['remote_password']
    back_up_sql = f"insert into table {table_name} select * from remote({remote_address,remote_table_name,remote_user,remote_password})"
    ck.execute_no_params(back_up_sql)

    check_sql_local = "select count() from {table_name}"
    check_sql_remote = "select count() from remote({remote_address,remote_table_name,remote_user,remote_password}"
    local_table_rows = ck.execute_no_params(check_sql_local)
    remote_table_rows = ck.execute_no_params(check_sql_remote)
    if local_table_rows[0][0] == remote_table_rows[0][0]:
        logger.info("backup data from remote server successfully")
    else:
        logger.info(f"local_table_rows : {local_table_rows}")
        logger.info(f"remote_table_rows : {remote_table_rows}")
        raise Exception("backup data from remote server unsuccessfully")