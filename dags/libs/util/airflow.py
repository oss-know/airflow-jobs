import airflow.providers.postgres.hooks.postgres as postgres_hooks


def get_postgres_conn():
    # 演示从airflow 获取 postgres_hooks 再获取 conn 链接
    # postgres_conn = postgres_hooks.PostgresHook.get_hook("airflow-jobs").get_conn()
    # pg_cursor = postgres_conn.cursor()
    # pg_cursor.execute("select version();")
    # record = pg_cursor.fetchone()
    # print("PostgresRecord:", record)
    # pg_cursor.close()
    # postgres_conn.close()
    postgres_conn = postgres_hooks.PostgresHook.get_hook("airflow-jobs").get_conn()
    return postgres_conn
