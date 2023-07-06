import mysql

from oss_know.libs.util.clickhouse_driver import CKServer


def calculate_xxx_metrics(owner, repo):
    return []


def save_xxx_metrics(mysql_conn_info, metrics):
    pass


class MetricRoutineCalculation:
    def __init__(self, clickhouse_conn_info, owner, repo, mysql_conn_info):
        self.clickhouse_client = CKServer(
            host=clickhouse_conn_info.get('HOST'),
            port=clickhouse_conn_info.get('PORT'),
            user=clickhouse_conn_info.get('USER'),
            password=clickhouse_conn_info.get('PASSWD'),
            database=clickhouse_conn_info.get('DATABASE'),
        )

        self.mysql_conn = mysql.connector.connect(
            host=mysql_conn_info['HOST'],
            port=mysql_conn_info['PORT'],
            user=mysql_conn_info['USER'],
            password=mysql_conn_info['PASSWORD'],
            database=mysql_conn_info['DATABASE'],
        )

        self.owner = owner
        self.repo = repo
        self.batch = []
        self.batch_size = 5000

    def calculate_metrics(self, force_update=False):
        print(f'calculating by {self.owner}, {self.repo}')
        if force_update:
            print('force update, remove the metrics calculated for previous period')
        return []

    def save_metrics(self):
        print(f'save metrics with {self.batch}')
        self.batch = []

    def routine(self):
        metrics = self.calculate_metrics()
        self.batch += metrics

        if len(self.batch) >= self.batch_size:
            self.save(metrics)
