from oss_know.libs.util.clickhouse_driver import CKServer


def get_uniq_owner_repos(clickhouse_conn_info, table_name):
    sql = f'''
    select distinct search_key__owner, search_key__repo from {table_name}
    '''
    ck_client = CKServer(
        host=clickhouse_conn_info.get('HOST'),
        port=clickhouse_conn_info.get('PORT'),
        user=clickhouse_conn_info.get('USER'),
        password=clickhouse_conn_info.get('PASSWD'),
        database=clickhouse_conn_info.get('DATABASE'),
    )

    result = ck_client.execute_no_params(sql)
    distinct_owner_repos = list(map(lambda tup: {'owner': tup[0], 'repo': tup[1]}, result))

    return distinct_owner_repos
