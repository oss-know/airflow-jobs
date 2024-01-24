import mysql.connector
from oss_know.libs.util.log import logger

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
        logger.info(f'calculating by {self.owner}, {self.repo}')

        # TODO We should set a dynamic condition to construct the SQL
        #  and control the length of output(don't be too larger than the batch_size)

        # TODO Calculate the metrics and assign them to var calculated_metrics
        calculated_metrics = []
        return calculated_metrics

    def save_metrics(self):
        logger.info(f'save metrics with {len(self.batch)} records, to {self.table_name}')
        self.batch = []

    def routine(self, force_update=True):
        if force_update:
            logger.info(f'{self.owner}/{self.repo}: force update, remove the metrics calculated for previous period')

        while True:
            metrics = self.calculate_metrics()
            if not metrics:
                break

            self.batch += metrics

            if len(self.batch) >= self.batch_size:
                self.save_metrics()

        if self.batch:
            self.save_metrics()
        self.clickhouse_client.close()
        self.mysql_conn.close()

    def routine_calculate_metrics_once(self):
        # 计算一次返回所有结果
        metrics = self.calculate_metrics()
        self.batch += metrics
        self.save_metrics()
        self.clickhouse_client.close()
        self.mysql_conn.close()

    def batch_insertion(self, insert_query, batch):
        cursor = self.mysql_conn.cursor()
        # 执行批量插入操作
        cursor.executemany(insert_query, batch)
        # 提交事务
        self.mysql_conn.commit()
        logger.info("Data inserted successfully!")
        cursor.close()


class TotalFixIndensityMetricRoutineCalculation(MetricRoutineCalculation):
    def calculate_metrics(self):
        # owner 必须是一个拥有很多个项目的社区
        sql_ = f"""

             --加入rust项目圈子后的总修复强度
        select author_email, count() as commit_count,ln(commit_count+1)
        from (select search_key__owner, search_key__repo, author_email, authored_date, b.*
              from (select *
                    from gits
                    where search_key__owner = '{self.owner}'
                      and length(parents) == 1
                      and author_email != '') as a global ASOF
                       INNER JOIN (select author_email, min(authored_date) as start_at, subtractYears(start_at, -1) as end_at
                                   from gits
                                   where search_key__owner = '{self.owner}'
                                     and length(parents) == 1
                                     and author_email != ''
                                   group by author_email) as b on a.author_email = b.author_email and a.authored_date <= end_at)
        group by author_email
        order by commit_count desc
            """
        results = self.clickhouse_client.execute_no_params(sql_)
        response = []
        for result in results:
            # result: (email, commit_count, intensity)
            # intensity = ln(commit_count + 1)
            response.append((self.owner,) + result)

        logger.info('calculating  Influence Metrics')
        return response
        # super().calculate_metrics()

    def save_metrics(self):
        logger.info('saving  Influence Metrics')
        total_fix_intensity_insert_query = '''
            INSERT INTO total_fix_intensity (owner, author_email, commit_count, total_fix_intensity)
            VALUES (%s, %s, %s, %s)'''
        self.batch_insertion(insert_query=total_fix_intensity_insert_query, batch=self.batch)


class DeveloperRoleMetricRoutineCalculation(MetricRoutineCalculation):
    def calculate_metrics(self):
        # 三个指标
        # owner 必须是一个拥有很多个项目的社区
        sql_ = f"""
            select a.*,b.repo_count from (select a.author_email,a.commit_count as total_fix_commit_count,b.commit_count as maximum_fix_commit_count   from (
        select author_email, count() as commit_count,ln(commit_count+1)
        from (select search_key__owner, search_key__repo, author_email, authored_date, b.*
              from (select *
                    from gits
                    where search_key__owner = '{self.owner}'
                      and length(parents) == 1
                      and author_email != '') as a global ASOF
                       INNER JOIN (select author_email, min(authored_date) as start_at, subtractYears(start_at, -1) as end_at
                                   from gits
                                   where search_key__owner = '{self.owner}'
                                     and length(parents) == 1
                                     and author_email != ''
                                   group by author_email) as b on a.author_email = b.author_email and a.authored_date <= end_at)
        group by author_email
        order by commit_count desc) as a global join ( select *
        from (select author_email, search_key__owner, search_key__repo, count() as commit_count, ln(commit_count + 1)
              from (select search_key__owner, search_key__repo, author_email, authored_date, b.*
                    from (select *
                          from gits
                          where search_key__owner = '{self.owner}'

                            and length(parents) == 1
                            and author_email != '') as a global ASOF
                             INNER JOIN (select author_email,
                                                min(authored_date)          as start_at,
                                                subtractYears(start_at, -1) as end_at
                                         from gits
                                         where search_key__owner = '{self.owner}'
                                           and length(parents) == 1
                                           and author_email != ''
                                         group by author_email) as b
                                        on a.author_email = b.author_email and a.authored_date <= end_at)
              group by author_email, search_key__owner, search_key__repo
              order by commit_count desc)
        limit 1 by author_email) as b on a.author_email = b.author_email) as a global join (select author_email, count() as repo_count,ln(repo_count+1)
        from (select author_email, search_key__owner, search_key__repo
              from (select search_key__owner, search_key__repo, author_email, authored_date, b.*
                    from (select *
                          from gits
                          where search_key__owner = '{self.owner}'
                            and length(parents) == 1
                            and author_email != '') as a global ASOF
                             INNER JOIN (select author_email,
                                                min(authored_date)          as start_at,
                                                subtractYears(start_at, -1) as end_at
                                         from gits
                                         where search_key__owner = '{self.owner}'
                                           and length(parents) == 1
                                           and author_email != ''
                                         group by author_email) as b
                                        on a.author_email = b.author_email and a.authored_date <= end_at)
              group by author_email, search_key__owner, search_key__repo)
        group by author_email order by repo_count desc) as b on a.author_email = b.author_email
            """
        results = self.clickhouse_client.execute_no_params(sql_)

        response = []
        for result in results:
            response.append((self.owner,) + result)

        logger.info('calculating  Influence Metrics')
        return response
        # super().calculate_metrics()

    def save_metrics(self):
        logger.info('saving  Influence Metrics')
        insert_query = '''
            INSERT INTO developer_roles_metrics (owner,author_email,total_fix_commit_count,maximum_fix_commit_count,repo_count)
            VALUES (%s, %s, %s, %s, %s)'''
        self.batch_insertion(insert_query=insert_query, batch=self.batch)


class ContributedRepoFirstYearMetricRoutineCalculation(MetricRoutineCalculation):
    def calculate_metrics(self):
        # 三个指标
        # owner 必须是一个拥有很多个项目的社区
        sql_ = f"""
                   --加入rust项目圈子后加入的总项目数
    select author_email, count() as repo_count
    from (select author_email, search_key__owner, search_key__repo
          from (select search_key__owner, search_key__repo, author_email, authored_date, b.*
                from (select *
                      from gits
                      where search_key__owner = '{self.owner}'
                        and length(parents) == 1
                        and author_email != '') as a global ASOF
                         INNER JOIN (select author_email,
                                            min(authored_date)          as start_at,
                                            subtractYears(start_at, -1) as end_at
                                     from gits
                                     where search_key__owner = '{self.owner}'
                                       and length(parents) == 1
                                       and author_email != ''
                                     group by author_email) as b
                                    on a.author_email = b.author_email and a.authored_date <= end_at)
          group by author_email, search_key__owner, search_key__repo)
    group by author_email order by repo_count desc
            """
        results = self.clickhouse_client.execute_no_params(sql_)

        response = []
        for result in results:
            response.append((self.owner,) + result)

        logger.info(f'Calculating contrib metrics {self.table_name}')
        return response
        # super().calculate_metrics()

    def save_metrics(self):
        logger.info(f'Saving contrib metrics {self.table_name}')
        insert_query = '''
            INSERT INTO contributed_repos_role (owner, author_email,repo_count)
            VALUES (%s, %s, %s)'''
        self.batch_insertion(insert_query=insert_query, batch=self.batch)


class BasicContributorGraphMetricRoutineCalculation(MetricRoutineCalculation):
    def calculate_metrics(self):
        # 每个项目都可以计算 author login ，repo， pr_count + issue_count + commit_count as total_contribute_count
        sql_ = f"""
            select *, pr_count + issue_count + commit_count as total_contribute_count
from (select
          if(a.user_login != '', a.user_login, b.author__login) as user_login,
             if(a.user_login != '', a.owner_repo, b.owner_repo)    as owner_repo,
             pr_count,
             issue_count,
             commit_count
      from (select if(a.user__login != '', a.user__login, b.user__login) as user_login,
                   if(a.user__login != '', a.owner_repo, b.owner_repo)   as owner_repo,
                   pr_count,
                   issue_count
            from (select user__login,
                         concat(search_key__owner, '__', search_key__repo) as owner_repo,
                         count()                                           as pr_count
                  from github_pull_requests
                  where search_key__owner = '{self.owner}'
                    and search_key__repo = '{self.repo}'
                --  and toYear(created_at) global in (2021, 2022)
                    and user__login not like '%[bot]%'
                  group by user__login, search_key__owner, search_key__repo
                  order by pr_count desc) as a global
                     full join (select user__login,
                                       concat(search_key__owner, '__', search_key__repo) as owner_repo,
                                       count()                                           as issue_count
                                from github_issues
                                where search_key__owner = '{self.owner}'
                                  and search_key__repo = '{self.repo}'
                               --   and toYear(created_at) global in (2021, 2022)
                                  and user__login not like '%[bot]%'
                                  and pull_request__url = ''
                                group by user__login, search_key__owner, search_key__repo
                                order by issue_count desc) as b
                               on a.user__login = b.user__login and a.owner_repo = b.owner_repo) as a
               global
               full join (select author__login,
                                 concat(search_key__owner, '__', search_key__repo) as owner_repo,
                                 count()                                           as commit_count
                          from github_commits
                          where search_key__owner = '{self.owner}'
                            and search_key__repo = '{self.repo}'
                          --  and toYear(commit__author__date) global in (2021, 2022)
                            and author__login not like '%[bot]%'
                          group by author__login, search_key__owner, search_key__repo
                          order by commit_count desc) as b
                         on a.user_login = b.author__login and a.owner_repo = b.owner_repo)
order by total_contribute_count desc
            """
        results = self.clickhouse_client.execute_no_params(sql_)

        response = []
        for result in results:
            response.append(result)

        logger.info('calculating  Influence Metrics')
        return response
        # super().calculate_metrics()

    def save_metrics(self):
        logger.info('saving  Influence Metrics')
        insert_query = '''
            INSERT INTO contribution_graph_data (cntrb_login,repo_name,pr_count, issue_count,commit_count,total_contributions)
            VALUES (%s, %s, %s, %s, %s, %s)'''
        self.batch_insertion(insert_query=insert_query, batch=self.batch)


class AverageRepairOfPeersMetricRoutineCalculation(MetricRoutineCalculation):
    def calculate_metrics(self):
        # 每个项目都可以计算
        sql_ = f"""
            --开发者在加入社区第一年，同行的平均贡献强度
select concat(search_key__owner, '__', search_key__repo) as owner_repo,dep_author_email,average_fix_intensity from (select search_key__owner,
       search_key__repo,
       dep_author_email,
       start_at,
       count()                               as commit_count,
       count(distinct all_author_email)      as author_count,
       round(commit_count / author_count, 2) as average_fix_intensity
from (select *
      from (select a.search_key__owner,
                   a.search_key__repo,
                   a.author_email as all_author_email,
                   a.authored_date,
                   b.author_email as dep_author_email,
                   b.commit_count,
                   b.start_at,
                   b.end_at
            from (select *
                  from gits
                  where search_key__owner = '{self.owner}'
                    and length(parents) == 1
                    and author_email != '') as a global
                     JOIN (select author_email,
                                  search_key__owner,
                                  search_key__repo,
                                  start_at,
                                  end_at,
                                  count() as commit_count,
                                  ln(commit_count + 1)
                           from (select search_key__owner, search_key__repo, author_email, authored_date, b.*
                                 from (select *
                                       from gits
                                       where search_key__owner = '{self.owner}'
                                         and search_key__repo != 'llvm-project'
                                         and search_key__repo != 'llvm'
                                         and search_key__repo != 'rust-gha'
                                         and length(parents) == 1
                                         and author_email != '') as a global ASOF
                                          inner JOIN (select author_email,
                                                             min(authored_date)          as start_at,
                                                             subtractYears(start_at, -1) as end_at
                                                      from gits
                                                      where search_key__owner = '{self.owner}'
                                                        and search_key__repo != 'llvm-project'
                                                        and search_key__repo != 'llvm'
                                                        and search_key__repo != 'rust-gha'
                                                        and length(parents) == 1
                                                        and author_email != ''
                                                      and author_email not like '%[bot]%'
                                                      group by author_email) as b
                                                     on a.author_email = b.author_email and a.authored_date <= end_at)
                           group by author_email, search_key__owner, search_key__repo, start_at, end_at
                           having commit_count > 5
                           order by author_email, start_at, commit_count desc
                           limit 1 by author_email) as b
                          on a.search_key__owner = b.search_key__owner and
                             a.search_key__repo = b.search_key__repo)
      where authored_date >= start_at
        and authored_date <= end_at
        and all_author_email != dep_author_email
        and all_author_email not like '%[bot]%'
      order by start_at)
group by search_key__owner, search_key__repo, dep_author_email, start_at)
            """
        results = self.clickhouse_client.execute_no_params(sql_)

        response = []
        for result in results:
            owner, repo = result[0].split('__')
            response.append((owner, repo, result[1], result[2]))

        logger.info('calculating Influence Metrics')
        return response

    def save_metrics(self):
        logger.info('saving  Influence Metrics')
        insert_query = '''
            INSERT INTO peers_average_fix_intensity_role (owner, repo, author_email, average_fix_intensity)
            VALUES (%s, %s, %s, %s)'''
        self.batch_insertion(insert_query=insert_query, batch=self.batch)


class MetricGroupRoutineCalculation:
    # TODO Pass concrete class as parameters for easier Calculation class fffff
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
            # owner is essential
            # While repo might be empty since some contribution metrics are calculated on community scale, which are
            # currently a group of repos under the same owner
            owner = item['owner']
            repo = item.get('repo', '')
            calc = self.calc_class(self.clickhouse_conn_info, self.mysql_conn_info,
                                   owner, repo, self.table_name, self.batch_size)
            calc.routine_calculate_metrics_once()

            if not repo:
                logger.info(
                    f'Calculating metrics {self.table_name} by community scale with {owner}/{repo}, skip other repos')
                break
