import mysql.connector


def get_mysql_conn(mysql_conn_info):
    host = mysql_conn_info.get('HOST')
    port = mysql_conn_info.get('PORT')
    user = mysql_conn_info.get('USER')
    password = mysql_conn_info.get('PASSWORD')
    database = mysql_conn_info.get('DATABASE')
    mysql_connector = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )
    return mysql_connector
