import mysql.connector

from oss_know.libs.util.clickhouse_driver import CKServer


def calculate_sample_influence_metrics(owner, repo):
    return []


def save_sample_influence_metrics(mysql_conn_info, metrics):
    pass


# Basic metric calculation class. The derived class should implement calculate_metrics and save_metrics
#  methods, which are scheduled by routine method
class MetricRoutineCalculation:
    def __init__(self, clickhouse_conn_info, mysql_conn_info, owner, repo, table_name, batch_size=5000):
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
        self.table_name = table_name
        self.batch_size = batch_size

    def calculate_metrics(self):
        print(f'calculating by {self.owner}, {self.repo}')

        # TODO We should set a dynamic condition to construct the SQL
        #  and control the length of output(don't be too larger than the batch_size)

        # TODO Calculate the metrics and assign them to var calculated_metrics
        calculated_metrics = []
        return calculated_metrics

    def save_metrics(self):
        print(f'save metrics with {len(self.batch)} records, to {self.table_name}')
        self.batch = []

    def routine(self, force_update=True):
        if force_update:
            print(f'{self.owner}/{self.repo}: force update, remove the metrics calculated for previous period')

        while True:
            metrics = self.calculate_metrics()
            if not metrics:
                break

            self.batch += metrics

            if len(self.batch) >= self.batch_size:
                self.save_metrics()

        if self.batch:
            self.save_metrics()


class SampleInfluenceMetricRoutineCalculation(MetricRoutineCalculation):
    def calculate_metrics(self):
        print('calculating Sample Influence Metrics')
        super().calculate_metrics()

    def save_metrics(self):
        print('saving Sample Influence Metrics')
        super().save_metrics()


class MetricGroupRoutineCalculation:
    # TODO Pass concrete class as parameters for easier Calculation class adoption
    def __init__(self, metric_calc_class,
                 clickhouse_conn_info, mysql_conn_info, owner_repos,
                 table_name, batch_size=5000):
        self.clickhouse_conn_info = clickhouse_conn_info
        self.mysql_conn_info = mysql_conn_info
        self.owner_repos = owner_repos
        self.batch = []
        self.table_name = table_name
        self.batch_size = batch_size
        self.calc_class = metric_calc_class

    def routine(self, force_update=False):
        for item in self.owner_repos:
            owner = item['owner']
            repo = item['repo']
            calc = self.calc_class(self.clickhouse_conn_info, self.mysql_conn_info,
                                   owner, repo, self.table_name, self.batch_size)
            calc.routine(force_update)
