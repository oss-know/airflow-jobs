import time
from datetime import datetime

from clickhouse_driver import Client, connect
from loguru import logger


class CKServer:
    def __init__(self, host, port, user, password, database, settings={}):
        self.client = Client(host=host, port=port, user=user, password=password, database=database, settings=settings)
        self.connect = connect(host=host, port=port, user=user, password=password, database=database)
        self.cursor = self.connect.cursor()

    def execute(self, sql: object, params: list) -> object:
        # self.cursor.execute(sql)
        # result = self.cursor.fetchall()
        result = self.client.execute(sql, params)
        return result

    def execute_use_setting(self, sql: object, params: list, settings) -> object:
        # self.cursor.execute(sql)
        # result = self.cursor.fetchall()
        result = self.client.execute(sql, params, settings=settings)
        return result

    def execute_no_params(self, sql: object):
        result = self.client.execute(sql)
        return result

    def fetchall(self, sql):
        result = self.client.execute(sql)
        return result

    def close(self):
        self.client.disconnect()


def get_metries_day_timeline_by_repo(ck, owner="", repo="", table_name=""):
    # owner = 'kubernetes'
    # repo = 'kubernetes'
    sql = f"""
    select 
    if(a.owner != '',
          a.owner,
          b.owner)        as owner,
       if(a.repo != '',
          a.repo,
          b.repo)         as repo,
       if(a.github_id != 0,
          a.commite_date,
          b.created_at)   as created_at,
       if(a.github_id != 0,
          a.github_id,
          b.github_id)    as github_id,

       commit_times,
       all_lines,
       file_counts,
       user_prs_counts,
       pr_body_length_avg,
       pr_body_length_sum,
       user_issues_counts,
       issue_body_length_avg,
       issue_body_length_sum,
       issues_comment_count,
       issues_comment_body_length_avg,
       issues_comment_body_length_sum,
       pr_comment_count,
       pr_comment_body_length_avg,
       pr_comment_body_length_sum,
       be_mentioned_times_in_issues,
       be_mentioned_times_in_pr,
       referred_other_issues_or_prs_in_issue,
       referred_other_issues_or_prs_in_pr,
       changed_label_times_in_issues,
       changed_label_times_in_prs,
       closed_issues_times,
       closed_prs_times
from
    --1 2 3
    (select owner,
            repo,
            commite_date,
            github_id,
            sum(commit_times) as commit_times,
            sum(all_lines)    as all_lines,
            sum(file_counts)  as file_counts
     from (select a.search_key__owner as owner,
                  a.search_key__repo  as repo,
                  a.commite_date,
                  b.author__id        as github_id,
                  author_email        as git_author_email,
                  commit_times,
                  all_lines,
                  file_counts
           from (
                    --按照邮箱项目分组统计提交代码次数和更改代码行数更改过多少的不同的文件
                    select commits_all_lines.*,
                           changed_files.file_counts
                    from (
                             -- 按照邮箱项目分组统计提交代码次数和更改代码行数
                             select search_key__owner,
                                    search_key__repo,
                                    author_email,
                                    toDate(committed_date) commite_date,
                                    COUNT(author_email) as commit_times,
                                    SUM(total__lines)   as all_lines
                             from gits
                             where search_key__owner = '{owner}'
                               and search_key__repo = '{repo}'
                               and author_email != ''
                             group by search_key__owner,
                                      search_key__repo,
                                      commite_date,
                                      author_email
                             ) commits_all_lines
                             GLOBAL
                             full JOIN
                         (
                             --统计这个人更改过多少的不同的文件
                             select search_key__owner,
                                    search_key__repo,
                                    commite_date,
                                    author_email,
                                    COUNT(*) file_counts
                             from (
                                      select DISTINCT `search_key__owner`,
                                                      `search_key__repo`,
                                                      `author_email`,
                                                      toDate(committed_date) commite_date,
                                                      `files.file_name` as   file_name
                                      from `gits`
                                               array join `files.file_name`
                                      where search_key__owner = '{owner}'
                                        and search_key__repo = '{repo}'
                                        and author_email != '')
                             group by search_key__owner,
                                      search_key__repo,
                                      commite_date,
                                      author_email) changed_files
                         ON
                                     commits_all_lines.search_key__owner = changed_files.search_key__owner
                                 AND
                                     commits_all_lines.search_key__repo = changed_files.search_key__repo
                                 AND
                                     commits_all_lines.author_email = changed_files.author_email
                                 and commits_all_lines.commite_date = changed_files.commite_date) a
                    GLOBAL
                    JOIN
                (
                    select DISTINCT search_key__owner,
                                    search_key__repo,
                                    commit__author__email,
                                    author__id
                    from github_commits gct
                    where author__id != 0
                      and search_key__owner = '{owner}'
                      and search_key__repo = '{repo}') b
                on
                            a.search_key__owner = b.search_key__owner
                        and a.search_key__repo = b.search_key__repo
                        and a.author_email = b.commit__author__email)
     group by owner, repo, commite_date, github_id
        ) a
        global
        full join
    -- 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19
        (select if(a.owner != '',
                   a.owner,
                   b.owner)        as owner,
                if(a.repo != '',
                   a.repo,
                   b.repo)         as repo,
                if(a.github_id != '',
                   a.created_at,
                   b.created_at)   as created_at,
                if(a.github_id != '',
                   toInt64(a.github_id),
                   b.github_id)    as github_id,

                user_prs_counts,
                pr_body_length_avg,
                pr_body_length_sum,
                user_issues_counts,
                issue_body_length_avg,
                issue_body_length_sum,
                issues_comment_count,
                issues_comment_body_length_avg,
                issues_comment_body_length_sum,
                pr_comment_count,
                pr_comment_body_length_avg,
                pr_comment_body_length_sum,
                be_mentioned_times_in_issues,
                be_mentioned_times_in_pr,
                referred_other_issues_or_prs_in_issue,
                referred_other_issues_or_prs_in_pr,
                changed_label_times_in_issues,
                changed_label_times_in_prs,
                closed_issues_times,
                closed_prs_times
         from
             --12 13 14 15 16 17 18 19
             (select if(a.owner != '',
                        a.owner,
                        b.owner)        as owner,
                     if(a.repo != '',
                        a.repo,
                        b.repo)         as repo,
                     if(a.github_id != '',
                        a.created_at,
                        b.created_at)   as created_at,
                     if(a.github_id != '',
                        a.github_id,
                        b.github_id)    as github_id,

                     be_mentioned_times_in_issues,
                     be_mentioned_times_in_pr,
                     referred_other_issues_or_prs_in_issue,
                     referred_other_issues_or_prs_in_pr,
                     changed_label_times_in_issues,
                     changed_label_times_in_prs,
                     closed_issues_times,
                     closed_prs_times

              from
                  --12 13 14 15
                  (select if(a.owner != '',
                             a.owner,
                             b.owner)        as owner,
                          if(a.repo != '',
                             a.repo,
                             b.repo)         as repo,
                          if(a.github_id != '',
                             a.created_at,
                             b.created_at)   as created_at,
                          if(a.github_id != '',
                             a.github_id,
                             b.github_id)    as github_id,

                          be_mentioned_times_in_issues,
                          be_mentioned_times_in_pr,
                          referred_other_issues_or_prs_in_issue,
                          referred_other_issues_or_prs_in_pr
                   from (
                            --12,13
                            select if(a.search_key__owner != '',
                                      a.search_key__owner,
                                      b.search_key__owner) as owner,
                                   if(a.search_key__repo != '',
                                      a.search_key__repo,
                                      b.search_key__repo)  as repo,
                                   if(a.id != '',
                                      a.created_at,
                                      b.created_at)        as created_at,
                                   if(a.id != '',
                                      a.id,
                                      b.id)                as github_id,

                                   be_mentioned_times_in_issues,
                                   be_mentioned_times_in_pr
                            from (
                                     select search_key__owner,
                                            search_key__repo,
                                            created_at,
                                            id,

                                            COUNT() as be_mentioned_times_in_issues
                                     from (
                                              select search_key__owner,
                                                     search_key__repo,
                                                     toDate(substring(JSONExtractString(timeline_raw,
                                                                                        'created_at'), 1, 10))
                                                                                as created_at,
                                                     JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'id')    as id

                                              from (
                                                       select github_issues_timeline.*
                                                       from (select *
                                                             from github_issues_timeline
                                                             where search_key__owner = '{owner}'
                                                               and search_key__repo = '{repo}'
                                                               and search_key__event = 'mentioned') github_issues_timeline global semi
                                                                left join (
                                                           select DISTINCT `number`,
                                                                           search_key__owner,
                                                                           search_key__repo
                                                           from github_issues gict
                                                           WHERE pull_request__url = ''
                                                             and search_key__owner = '{owner}'
                                                             and search_key__repo = '{repo}') as issues_number
                                                                          on
                                                                              github_issues_timeline.search_key__number = issues_number.number
                                                       )
                                              WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                              'actor'),
                                                            'id') = 1
                                              )
                                     group by search_key__owner,
                                              search_key__repo,
                                              created_at,
                                              id
                                              ) a
                                     GLOBAL
                                     FULL JOIN
                                 (
                                     select search_key__owner,
                                            search_key__repo,
                                            created_at,
                                            id,

                                            COUNT() as be_mentioned_times_in_pr
                                     from (
                                              select search_key__owner,
                                                     search_key__repo,
                                                     toDate(substring(JSONExtractString(timeline_raw,
                                                                                        'created_at'), 1, 10))
                                                                                as created_at,
                                                     JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'id')    as id
                                              from (
                                                       select github_issues_timeline.*
                                                       from (select *
                                                             from github_issues_timeline
                                                             where search_key__owner = '{owner}'
                                                               and search_key__repo = '{repo}'
                                                               and search_key__event = 'mentioned') github_issues_timeline global semi
                                                                left join (
                                                           select DISTINCT `number`,
                                                                           search_key__owner,
                                                                           search_key__repo
                                                           from github_issues gict
                                                           WHERE pull_request__url != ''
                                                             and search_key__owner = '{owner}'
                                                             and search_key__repo = '{repo}') as pr_number
                                                                          on
                                                                              github_issues_timeline.search_key__number = pr_number.number
                                                       )
                                              WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                              'actor'),
                                                            'id') = 1
                                              )
                                     group by search_key__owner,
                                              search_key__repo,
                                              created_at,
                                              id
                                              ) b
                                 ON
                                             a.id = b.id

                                         and a.created_at = b.created_at
                                         and a.search_key__owner = b.search_key__owner
                                         and a.search_key__repo = b.search_key__repo) a
                            GLOBAL
                            FULL JOIN
                        (
                            --14 15
                            select if(a.search_key__owner != '',
                                      a.search_key__owner,
                                      b.search_key__owner) as owner,
                                   if(a.search_key__repo != '',
                                      a.search_key__repo,
                                      b.search_key__repo)  as repo,
                                   if(a.id != '',
                                      a.created_at,
                                      b.created_at)        as created_at,
                                   if(a.id != '',
                                      a.id,
                                      b.id)                as github_id,

                                   referred_other_issues_or_prs_in_issue,
                                   referred_other_issues_or_prs_in_pr
                            from (
                                     select cross_referenced.search_key__owner,
                                            cross_referenced.search_key__repo,
                                            created_at,
                                            id,

                                            COUNT(id) as referred_other_issues_or_prs_in_issue
                                     from (
                                              select search_key__owner,
                                                     search_key__repo,
                                                     toDate(substring(JSONExtractString(timeline_raw,
                                                                                        'created_at'), 1, 10))
                                                                                as created_at,
                                                     JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'id')    as id
                                              from (select github_issues_timeline.*
                                                    from (select *,
                                                                 JSONExtractString(
                                                                         JSONExtractString(
                                                                                 JSONExtractString(timeline_raw,
                                                                                                   'source'),
                                                                                 'issue'),
                                                                         'number') as number
                                                          from github_issues_timeline
                                                          where search_key__owner = '{owner}'
                                                            and search_key__repo = '{repo}'
                                                            and search_key__event = 'cross-referenced') github_issues_timeline GLOBAL
                                                             JOIN
                                                         (
                                                             select DISTINCT `number`,
                                                                             search_key__owner,
                                                                             search_key__repo
                                                             from github_issues gict
                                                             WHERE pull_request__url = ''
                                                               and search_key__owner = '{owner}'
                                                               and search_key__repo = '{repo}') issues
                                                         on github_issues_timeline.search_key__owner =
                                                            issues.search_key__owner
                                                             and
                                                            github_issues_timeline.search_key__repo =
                                                            issues.search_key__repo
                                                             and
                                                            github_issues_timeline.number = toString(issues.number))

                                              WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                              'actor'),
                                                            'id') = 1) cross_referenced

                                     GROUP BY cross_referenced.search_key__owner,
                                              cross_referenced.search_key__repo,
                                              created_at,
                                              id
                                              ) a
                                     GLOBAL
                                     FULL JOIN
                                 (
                                     select cross_referenced.search_key__owner,
                                            cross_referenced.search_key__repo,
                                            created_at,
                                            id,

                                            COUNT(id) as referred_other_issues_or_prs_in_pr
                                     from (
                                              select search_key__owner,
                                                     search_key__repo,
                                                     toDate(substring(JSONExtractString(timeline_raw,
                                                                                        'created_at'), 1, 10))
                                                                                as created_at,
                                                     JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'id')    as id

                                              from (select *
                                                    from (select *,
                                                                 JSONExtractString(
                                                                         JSONExtractString(
                                                                                 JSONExtractString(timeline_raw,
                                                                                                   'source'),
                                                                                 'issue'),
                                                                         'number') as number
                                                          from github_issues_timeline
                                                          where search_key__owner = '{owner}'
                                                            and search_key__repo = '{repo}'
                                                            and search_key__event = 'cross-referenced') github_issues_timeline GLOBAL
                                                             JOIN
                                                         (
                                                             select DISTINCT `number`,
                                                                             search_key__owner,
                                                                             search_key__repo
                                                             from github_issues gict
                                                             WHERE pull_request__url != ''
                                                               and search_key__owner = '{owner}'
                                                               and search_key__repo = '{repo}') prs
                                                         ON
                                                                     github_issues_timeline.search_key__owner =
                                                                     prs.search_key__owner
                                                                 and
                                                                     github_issues_timeline.search_key__repo =
                                                                     prs.search_key__repo
                                                                 and
                                                                     github_issues_timeline.number =
                                                                     toString(prs.number))
                                              WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                              'actor'),
                                                            'id') = 1) cross_referenced

                                     GROUP BY cross_referenced.search_key__owner,
                                              cross_referenced.search_key__repo,
                                              created_at,
                                              id
                                              ) b
                                 ON
                                             a.id = b.id

                                         and a.created_at = b.created_at
                                         and a.search_key__owner = b.search_key__owner
                                         and a.search_key__repo = b.search_key__repo) b
                        ON
                                    a.github_id = b.github_id

                                and a.created_at = b.created_at
                                and a.owner = b.owner
                                and a.repo = b.repo) a
                      global
                      full join
                  --16 17 18 19
                      (select if(a.owner != '',
                                 a.owner,
                                 b.owner)        as owner,
                              if(a.repo != '',
                                 a.repo,
                                 b.repo)         as repo,
                              if(a.github_id != '',
                                 a.created_at,
                                 b.created_at)   as created_at,
                              if(a.github_id != '',
                                 a.github_id,
                                 b.github_id)    as github_id,

                              changed_label_times_in_issues,
                              changed_label_times_in_prs,
                              closed_issues_times,
                              closed_prs_times
                       from (
                                --12 13
                                select if(a.search_key__owner != '',
                                          a.search_key__owner,
                                          b.search_key__owner) as owner,
                                       if(a.search_key__repo != '',
                                          a.search_key__repo,
                                          b.search_key__repo)  as repo,
                                       if(a.id != '',
                                          a.created_at,
                                          b.created_at)        as created_at,
                                       if(a.id != '',
                                          a.id,
                                          b.id)                as github_id,

                                       changed_label_times_in_issues,
                                       changed_label_times_in_prs
                                from (
                                         select github_issues_timeline.search_key__owner,
                                                github_issues_timeline.search_key__repo,
                                                toDate(substring(JSONExtractString(timeline_raw,
                                                                                   'created_at'), 1, 10))
                                                                           as created_at,
                                                JSONExtractString(JSONExtractString(timeline_raw,
                                                                                    'actor'),
                                                                  'id')    as id,

                                                count(id)                  as changed_label_times_in_issues
                                         from (select *
                                               from github_issues_timeline
                                               where search_key__owner = '{owner}'
                                                 and search_key__repo = '{repo}'
                                                 and (search_key__event = 'labeled' or search_key__event = 'unlabeled')) github_issues_timeline
                                                  global
                                                  join
                                              (
                                                  select DISTINCT `number`,
                                                                  search_key__owner,
                                                                  search_key__repo
                                                  from github_issues gict
                                                  WHERE pull_request__url = ''
                                                    and search_key__owner = '{owner}'
                                                    and search_key__repo = '{repo}') as issues_number
                                              on
                                                          github_issues_timeline.search_key__number =
                                                          issues_number.number
                                                      and
                                                          github_issues_timeline.search_key__owner =
                                                          issues_number.search_key__owner
                                                      and
                                                          github_issues_timeline.search_key__repo =
                                                          issues_number.search_key__repo
                                         WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                         'actor'),
                                                       'id') = 1
                                         GROUP BY github_issues_timeline.search_key__owner,
                                                  github_issues_timeline.search_key__repo,
                                                  created_at,
                                                  id
                                                  ) a
                                         GLOBAL
                                         FULL JOIN
                                     (
                                         select github_issues_timeline.search_key__owner,
                                                github_issues_timeline.search_key__repo,
                                                toDate(substring(JSONExtractString(timeline_raw,
                                                                                   'created_at'), 1, 10))
                                                                           as created_at,
                                                JSONExtractString(JSONExtractString(timeline_raw,
                                                                                    'actor'),
                                                                  'id')    as id,

                                                count(id)                  as changed_label_times_in_prs
                                         from (select *
                                               from github_issues_timeline
                                               where search_key__owner = '{owner}'
                                                 and search_key__repo = '{repo}'
                                                 and (search_key__event = 'labeled' or search_key__event = 'unlabeled')) github_issues_timeline
                                                  global
                                                  join
                                              (
                                                  select DISTINCT `number`,
                                                                  search_key__owner,
                                                                  search_key__repo
                                                  from github_issues gict
                                                  WHERE pull_request__url != ''
                                                    and search_key__owner = '{owner}'
                                                    and search_key__repo = '{repo}') as pr_number
                                              on
                                                          github_issues_timeline.search_key__number = pr_number.number
                                                      and
                                                          github_issues_timeline.search_key__owner =
                                                          pr_number.search_key__owner
                                                      and
                                                          github_issues_timeline.search_key__repo =
                                                          pr_number.search_key__repo
                                         WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                         'actor'),
                                                       'id') = 1
                                         GROUP BY github_issues_timeline.search_key__owner,
                                                  github_issues_timeline.search_key__repo,
                                                  created_at,
                                                  id) b
                                     ON
                                                 a.id = b.id
                                             and a.search_key__owner = b.search_key__owner
                                             and a.search_key__repo = b.search_key__repo
                                             and a.created_at = b.created_at) a
                                GLOBAL
                                FULL JOIN
                            (
                                --14 15
                                select if(a.search_key__owner != '',
                                          a.search_key__owner,
                                          b.search_key__owner) as owner,
                                       if(a.search_key__repo != '',
                                          a.search_key__repo,
                                          b.search_key__repo)  as repo,
                                       if(a.actor_id != '',
                                          a.created_at,
                                          b.created_at)        as created_at,
                                       if(a.actor_id != '',
                                          a.actor_id,
                                          b.actor_id)          as github_id,

                                       closed_issues_times,
                                       closed_prs_times
                                from (
                                         select search_key__owner,
                                                search_key__repo,
                                                toDate(substring(JSONExtractString(timeline_raw,
                                                                                   'created_at'), 1, 10))
                                                                           as created_at,
                                                JSONExtractString(JSONExtractString(timeline_raw,
                                                                                    'actor'),
                                                                  'id')    as actor_id,

                                                COUNT()                       closed_issues_times
                                         from (
                                                  select github_issues_timeline.*
                                                  from (select *
                                                        from github_issues_timeline
                                                        where search_key__owner = '{owner}'
                                                          and search_key__repo = '{repo}'
                                                          and search_key__event = 'closed') github_issues_timeline global
                                                           join (
                                                      select DISTINCT `number`,
                                                                      search_key__owner,
                                                                      search_key__repo
                                                      from github_issues gict
                                                      WHERE pull_request__url = ''
                                                        and search_key__owner = '{owner}'
                                                        and search_key__repo = '{repo}') as issues_number
                                                                on
                                                                            github_issues_timeline.search_key__number =
                                                                            issues_number.number
                                                                        and github_issues_timeline.search_key__owner =
                                                                            issues_number.search_key__owner
                                                                        and github_issues_timeline.search_key__repo =
                                                                            issues_number.search_key__repo)
                                         WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                         'actor'),
                                                       'id') = 1
                                         group by search_key__owner,
                                                  search_key__repo,
                                                  created_at,
                                                  actor_id) a
                                         GLOBAL
                                         FULL JOIN
                                     (
                                         select search_key__owner,
                                                search_key__repo,
                                                toDate(substring(JSONExtractString(timeline_raw,
                                                                                   'created_at'), 1, 10))
                                                                           as created_at,
                                                JSONExtractString(JSONExtractString(timeline_raw,
                                                                                    'actor'),
                                                                  'id')    as actor_id,

                                                COUNT()                       closed_prs_times
                                         from (
                                                  select github_issues_timeline.*
                                                  from (select *
                                                        from github_issues_timeline
                                                        where search_key__owner = '{owner}'
                                                          and search_key__repo = '{repo}'
                                                          and search_key__event = 'closed') github_issues_timeline global
                                                           join (
                                                      select DISTINCT `number`,
                                                                      search_key__owner,
                                                                      search_key__repo
                                                      from github_issues gict
                                                      WHERE pull_request__url != ''
                                                        and search_key__owner = '{owner}'
                                                        and search_key__repo = '{repo}') as pr_number
                                                                on
                                                                            github_issues_timeline.search_key__number =
                                                                            pr_number.number
                                                                        and github_issues_timeline.search_key__owner =
                                                                            pr_number.search_key__owner
                                                                        and github_issues_timeline.search_key__repo =
                                                                            pr_number.search_key__repo)
                                         WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                         'actor'),
                                                       'id') = 1
                                         group by search_key__owner,
                                                  search_key__repo,
                                                  created_at,
                                                  actor_id) b
                                     ON
                                                 a.actor_id = b.actor_id
                                             and a.created_at = b.created_at
                                             and a.search_key__owner = b.search_key__owner
                                             and a.search_key__repo = b.search_key__repo) b
                            ON
                                        a.github_id = b.github_id
                                    and a.created_at = b.created_at
                                    and a.owner = b.owner
                                    and a.repo = b.repo) b
                  on
                              a.github_id = b.github_id and
                              a.created_at = b.created_at) a
                 global
                 full join
             -- 4 5 6 7 8 9 10 11
                 (select if(a.owner != '',
                            a.owner,
                            b.owner)        as owner,
                         if(a.repo != '',
                            a.repo,
                            b.repo)         as repo,
                         if(a.github_id != 0,
                            a.created_at,
                            b.created_at)   as created_at,
                         if(a.github_id != 0,
                            a.github_id,
                            b.github_id)    as github_id,
                         user_prs_counts,
                         pr_body_length_avg,
                         pr_body_length_sum,
                         user_issues_counts,
                         issue_body_length_avg,
                         issue_body_length_sum,
                         issues_comment_count,
                         issues_comment_body_length_avg,
                         issues_comment_body_length_sum,
                         pr_comment_count,
                         pr_comment_body_length_avg,
                         pr_comment_body_length_sum

                  from
                      -- 4 5 6 7
                      (select if(a.search_key__owner != '',
                                 a.search_key__owner,
                                 b.search_key__owner) as owner,
                              if(a.search_key__repo != '',
                                 a.search_key__repo,
                                 b.search_key__repo)  as repo,
                              if(a.user__id != 0,
                                 a.created_at,
                                 b.created_at)        as created_at,
                              if(a.user__id != 0,
                                 a.user__id,
                                 b.user__id)          as github_id,
                              a.user_prs_counts,
                              a.body_length_avg       as pr_body_length_avg,
                              a.body_length_sum       as pr_body_length_sum,
                              b.user_issues_counts,
                              b.body_length_avg       as issue_body_length_avg,
                              b.body_length_sum       as issue_body_length_sum
                       from (
                                --4,5
                                select `search_key__owner`,
                                       `search_key__repo`,
                                       toDate(created_at)    created_at,
                                       `user__id`,
                                       COUNT(user__id) as    `user_prs_counts`,
                                       sum(lengthUTF8(body)) body_length_sum,
                                       avg(lengthUTF8(body)) `body_length_avg`
                                from github_pull_requests
                                where search_key__owner = '{owner}'
                                  and search_key__repo = '{repo}'
                                group by `search_key__owner`,
                                         `search_key__repo`,
                                         created_at,
                                         `user__id`) a
                                GLOBAL
                                FULL JOIN
                            (
                                -- 6 7
                                select `search_key__owner`,
                                       `search_key__repo`,
                                       toDate(created_at)    created_at,
                                       `user__id`,
                                       COUNT(user__id) as    `user_issues_counts`,
                                       sum(lengthUTF8(body)) body_length_sum,
                                       avg(lengthUTF8(body)) `body_length_avg`
                                from github_issues
                                where pull_request__url == ''
                                  and search_key__owner = '{owner}'
                                  and search_key__repo = '{repo}'
                                group by `search_key__owner`,
                                         `search_key__repo`,
                                         created_at,
                                         `user__id`) b
                            ON
                                        a.user__id = b.user__id
                                    and a.created_at = b.created_at
                                    and a.search_key__owner = b.search_key__owner
                                    and a.search_key__repo = b.search_key__repo) a
                          global
                          full join
                      -- 8 9 10 11
                          (select if(a.search_key__owner != '',
                                     a.search_key__owner,
                                     b.search_key__owner) as owner,
                                  if(a.search_key__repo != '',
                                     a.search_key__repo,
                                     b.search_key__repo)  as repo,
                                  if(a.user__id != 0,
                                     a.created_at,
                                     b.created_at)        as created_at,
                                  if(a.user__id != 0,
                                     a.user__id,
                                     b.user__id)          as github_id,

                                  a.issues_comment_count,
                                  a.issues_comment_body_length_avg,
                                  a.body_length_sum       as issues_comment_body_length_sum,
                                  b.pr_comment_count,
                                  b.pr_comment_body_length_avg,
                                  b.body_length_sum       as pr_comment_body_length_sum
                           from (
                                    select search_key__owner,
                                           search_key__repo,
                                           toDate(created_at)    created_at,
                                           user__id,

                                           COUNT() as            issues_comment_count,
                                           sum(lengthUTF8(body)) body_length_sum,
                                           avg(lengthUTF8(body)) issues_comment_body_length_avg
                                    from (
                                             select github_issues_comments.*
                                             from (select *
                                                   from github_issues_comments
                                                   where search_key__owner = '{owner}'
                                                     and search_key__repo = '{repo}') github_issues_comments global
                                                      semi
                                                      left join (
                                                 select DISTINCT `number`,
                                                                 search_key__owner,
                                                                 search_key__repo
                                                 from github_issues gict
                                                 WHERE pull_request__url = ''
                                                   and search_key__owner = '{owner}'
                                                   and search_key__repo = '{repo}') as pr_number
                                                                on
                                                                    github_issues_comments.search_key__number = pr_number.number
                                             )
                                    group by search_key__owner,
                                             search_key__repo,
                                             created_at,
                                             user__id) a
                                    GLOBAL
                                    FULL JOIN
                                (
                                    select search_key__owner,
                                           search_key__repo,
                                           toDate(created_at)    created_at,
                                           user__id,
                                           COUNT() as            pr_comment_count,
                                           sum(lengthUTF8(body)) body_length_sum,
                                           avg(lengthUTF8(body)) pr_comment_body_length_avg
                                    from (
                                             select github_issues_comments.*
                                             from (select *
                                                   from github_issues_comments
                                                   where search_key__owner = '{owner}'
                                                     and search_key__repo = '{repo}') github_issues_comments global
                                                      semi
                                                      left join (
                                                 select DISTINCT `number`,
                                                                 search_key__owner,
                                                                 search_key__repo
                                                 from github_issues gict
                                                 WHERE pull_request__url != ''
                                                   and search_key__owner = '{owner}'
                                                   and search_key__repo = '{repo}') as pr_number
                                                                on
                                                                    github_issues_comments.search_key__number = pr_number.number
                                             )
                                    group by search_key__owner,
                                             search_key__repo,
                                             created_at,
                                             user__id) b
                                ON
                                            a.user__id = b.user__id and
                                            a.created_at = b.created_at
                                        and a.search_key__owner = b.search_key__owner
                                        and a.search_key__repo = b.search_key__repo) b
                      on
                                  a.github_id = b.github_id and
                                  a.created_at = b.created_at) b
             on
                         toInt64(a.github_id) = b.github_id and

                         a.created_at = b.created_at) b
    on
                a.github_id = b.github_id and

                a.commite_date = b.created_at;



"""
    # table_name = 'metrics_day_timeline'
    insert_sql = f"insert into {table_name} values"
    logger.info(f"计算owner: {owner},repo: {repo}        metrics......")
    results = ck.execute_no_params(sql)
    bulk_data = []
    count = 0
    for result in results:
        count += 1
        data_dict = {}
        data_dict["ck_data_insert_at"] = int(time.time() * 1000)
        data_dict["owner"] = result[0]
        data_dict["repo"] = result[1]
        data_dict["created_at"] = datetime.combine(result[2], datetime.min.time())
        data_dict["github_id"] = result[3]
        # data_dict["github_login"] = result[4]
        # data_dict["git_author_email"] = result[5]
        data_dict["commit_times"] = result[4]
        data_dict["changed_lines"] = result[5]
        data_dict["diff_file_counts"] = result[6]
        data_dict["prs_counts"] = result[7]
        data_dict["pr_body_length_avg"] = result[8]
        data_dict["pr_body_length_sum"] = result[9]
        data_dict["issues_counts"] = result[10]
        data_dict["issue_body_length_avg"] = result[11]
        data_dict["issue_body_length_sum"] = result[12]
        data_dict["issues_comment_count"] = result[13]
        data_dict["issues_comment_body_length_avg"] = result[14]
        data_dict["issues_comment_body_length_sum"] = result[15]
        data_dict["pr_comment_count"] = result[16]
        data_dict["pr_comment_body_length_avg"] = result[17]
        data_dict["pr_comment_body_length_sum"] = result[18]
        data_dict["be_mentioned_times_in_issues"] = result[19]
        data_dict["be_mentioned_times_in_pr"] = result[20]
        data_dict["referred_other_issues_or_prs_in_issue"] = result[21]
        data_dict["referred_other_issues_or_prs_in_pr"] = result[22]
        data_dict["changed_label_times_in_issues"] = result[23]
        data_dict["changed_label_times_in_prs"] = result[24]
        data_dict["closed_issues_times"] = result[25]
        data_dict["closed_prs_times"] = result[26]

        bulk_data.append(data_dict)

        if len(bulk_data) == 20000:
            ck.execute(sql=insert_sql, params=bulk_data)
            logger.info(f"已经插入{count}条")
            bulk_data.clear()
    if bulk_data:
        ck.execute(sql=insert_sql, params=bulk_data)
        logger.info(f"已经插入{count}条")
    logger.info("wait 30 sec........")
    time.sleep(30)
    if not if_data_eq_github(count=count, ck=ck, table_name=table_name, owner=owner, repo=repo):
        raise Exception("Insert failed：Inconsistent data count")
    else:
        logger.info("Successfully inserted: datas are consistent")
    ck.close()



def get_metries_month_timeline_by_repo(ck, owner="", repo="", table_name=""):
    # owner = 'kubernetes'
    # repo = 'kubernetes'
    sql = f"""
select if(a.owner != '',
          a.owner,
          b.owner)        as owner,
       if(a.repo != '',
          a.repo,
          b.repo)         as repo,
       if(a.github_id != 0,
          a.commite_date,
          b.created_at)   as created_at,
       if(a.github_id != 0,
          a.github_id,
          b.github_id)    as github_id,

       commit_times,
       all_lines,
       file_counts,
       user_prs_counts,
       pr_body_length_avg,
       pr_body_length_sum,
       user_issues_counts,
       issue_body_length_avg,
       issue_body_length_sum,
       issues_comment_count,
       issues_comment_body_length_avg,
       issues_comment_body_length_sum,
       pr_comment_count,
       pr_comment_body_length_avg,
       pr_comment_body_length_sum,
       be_mentioned_times_in_issues,
       be_mentioned_times_in_pr,
       referred_other_issues_or_prs_in_issue,
       referred_other_issues_or_prs_in_pr,
       changed_label_times_in_issues,
       changed_label_times_in_prs,
       closed_issues_times,
       closed_prs_times
from
    --1 2 3
    (select owner,
            repo,
            commite_date,
            github_id,
            sum(commit_times) as commit_times,
            sum(all_lines)    as all_lines,
            sum(file_counts)  as file_counts
     from (select a.search_key__owner as owner,
                  a.search_key__repo  as repo,
                  a.commite_date,
                  b.author__id        as github_id,
                  author_email        as git_author_email,
                  commit_times,
                  all_lines,
                  file_counts
           from (
                    --按照邮箱项目分组统计提交代码次数和更改代码行数更改过多少的不同的文件
                    select commits_all_lines.*,
                           changed_files.file_counts
                    from (
                             -- 按照邮箱项目分组统计提交代码次数和更改代码行数
                             select search_key__owner,
                                    search_key__repo,
                                    author_email,
                                    toYYYYMM(toDate(committed_date))
                                     commite_date,
                                    COUNT(author_email) as commit_times,
                                    SUM(total__lines)   as all_lines
                             from gits
                             where search_key__owner = '{owner}'
                               and search_key__repo = '{repo}'
                               and author_email != ''
                             group by search_key__owner,
                                      search_key__repo,
                                      commite_date,
                                      author_email
                             ) commits_all_lines
                             GLOBAL
                             full JOIN
                         (
                             --统计这个人更改过多少的不同的文件
                             select search_key__owner,
                                    search_key__repo,
                                    commite_date,
                                    author_email,
                                    COUNT(*) file_counts
                             from (
                                      select DISTINCT `search_key__owner`,
                                                      `search_key__repo`,
                                                      `author_email`,
                                                      toYYYYMM( toDate(committed_date))
                                                      commite_date,
                                                      `files.file_name` as   file_name
                                      from `gits`
                                               array join `files.file_name`
                                      where search_key__owner = '{owner}'
                                        and search_key__repo = '{repo}'
                                        and author_email != '')
                             group by search_key__owner,
                                      search_key__repo,
                                      commite_date,
                                      author_email) changed_files
                         ON
                                     commits_all_lines.search_key__owner = changed_files.search_key__owner
                                 AND
                                     commits_all_lines.search_key__repo = changed_files.search_key__repo
                                 AND
                                     commits_all_lines.author_email = changed_files.author_email
                                 and commits_all_lines.commite_date = changed_files.commite_date) a
                    GLOBAL
                    JOIN
                (
                    select DISTINCT search_key__owner,
                                    search_key__repo,
                                    commit__author__email,
                                    author__id
                    from github_commits gct
                    where author__id != 0
                      and search_key__owner = '{owner}'
                      and search_key__repo = '{repo}') b
                on
                            a.search_key__owner = b.search_key__owner
                        and a.search_key__repo = b.search_key__repo
                        and a.author_email = b.commit__author__email)
     group by owner, repo, commite_date, github_id
        ) a
        global
        full join
    -- 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19
        (select if(a.owner != '',
                   a.owner,
                   b.owner)        as owner,
                if(a.repo != '',
                   a.repo,
                   b.repo)         as repo,
                if(a.github_id != '',
                   a.created_at,
                   b.created_at)   as created_at,
                if(a.github_id != '',
                   toInt64(a.github_id),
                   b.github_id)    as github_id,

                user_prs_counts,
                pr_body_length_avg,
                pr_body_length_sum,
                user_issues_counts,
                issue_body_length_avg,
                issue_body_length_sum,
                issues_comment_count,
                issues_comment_body_length_avg,
                issues_comment_body_length_sum,
                pr_comment_count,
                pr_comment_body_length_avg,
                pr_comment_body_length_sum,
                be_mentioned_times_in_issues,
                be_mentioned_times_in_pr,
                referred_other_issues_or_prs_in_issue,
                referred_other_issues_or_prs_in_pr,
                changed_label_times_in_issues,
                changed_label_times_in_prs,
                closed_issues_times,
                closed_prs_times
         from
             --12 13 14 15 16 17 18 19
             (select if(a.owner != '',
                        a.owner,
                        b.owner)        as owner,
                     if(a.repo != '',
                        a.repo,
                        b.repo)         as repo,
                     if(a.github_id != '',
                        a.created_at,
                        b.created_at)   as created_at,
                     if(a.github_id != '',
                        a.github_id,
                        b.github_id)    as github_id,

                     be_mentioned_times_in_issues,
                     be_mentioned_times_in_pr,
                     referred_other_issues_or_prs_in_issue,
                     referred_other_issues_or_prs_in_pr,
                     changed_label_times_in_issues,
                     changed_label_times_in_prs,
                     closed_issues_times,
                     closed_prs_times

              from
                  --12 13 14 15
                  (select if(a.owner != '',
                             a.owner,
                             b.owner)        as owner,
                          if(a.repo != '',
                             a.repo,
                             b.repo)         as repo,
                          if(a.github_id != '',
                             a.created_at,
                             b.created_at)   as created_at,
                          if(a.github_id != '',
                             a.github_id,
                             b.github_id)    as github_id,

                          be_mentioned_times_in_issues,
                          be_mentioned_times_in_pr,
                          referred_other_issues_or_prs_in_issue,
                          referred_other_issues_or_prs_in_pr
                   from (
                            --12,13
                            select if(a.search_key__owner != '',
                                      a.search_key__owner,
                                      b.search_key__owner) as owner,
                                   if(a.search_key__repo != '',
                                      a.search_key__repo,
                                      b.search_key__repo)  as repo,
                                   if(a.id != '',
                                      a.created_at,
                                      b.created_at)        as created_at,
                                   if(a.id != '',
                                      a.id,
                                      b.id)                as github_id,

                                   be_mentioned_times_in_issues,
                                   be_mentioned_times_in_pr
                            from (
                                     select search_key__owner,
                                            search_key__repo,
                                            created_at,
                                            id,

                                            COUNT() as be_mentioned_times_in_issues
                                     from (
                                              select search_key__owner,
                                                     search_key__repo,
                                                     toYYYYMM(toDate(substring(JSONExtractString(timeline_raw,
                                                                                        'created_at'), 1, 10)))

                                                                                as created_at,
                                                     JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'id')    as id

                                              from (
                                                       select github_issues_timeline.*
                                                       from (select *
                                                             from github_issues_timeline
                                                             where search_key__owner = '{owner}'
                                                               and search_key__repo = '{repo}'
                                                               and search_key__event = 'mentioned') github_issues_timeline global semi
                                                                left join (
                                                           select DISTINCT `number`,
                                                                           search_key__owner,
                                                                           search_key__repo
                                                           from github_issues gict
                                                           WHERE pull_request__url = ''
                                                             and search_key__owner = '{owner}'
                                                             and search_key__repo = '{repo}') as issues_number
                                                                          on
                                                                              github_issues_timeline.search_key__number = issues_number.number
                                                       )
                                              WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                              'actor'),
                                                            'id') = 1
                                              )
                                     group by search_key__owner,
                                              search_key__repo,
                                              created_at,
                                              id
                                              ) a
                                     GLOBAL
                                     FULL JOIN
                                 (
                                     select search_key__owner,
                                            search_key__repo,
                                            created_at,
                                            id,

                                            COUNT() as be_mentioned_times_in_pr
                                     from (
                                              select search_key__owner,
                                                     search_key__repo,
                                                     toYYYYMM(toDate(substring(JSONExtractString(timeline_raw,
                                                                                        'created_at'), 1, 10)))

                                                                                as created_at,
                                                     JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'id')    as id
                                              from (
                                                       select github_issues_timeline.*
                                                       from (select *
                                                             from github_issues_timeline
                                                             where search_key__owner = '{owner}'
                                                               and search_key__repo = '{repo}'
                                                               and search_key__event = 'mentioned') github_issues_timeline global semi
                                                                left join (
                                                           select DISTINCT `number`,
                                                                           search_key__owner,
                                                                           search_key__repo
                                                           from github_issues gict
                                                           WHERE pull_request__url != ''
                                                             and search_key__owner = '{owner}'
                                                             and search_key__repo = '{repo}') as pr_number
                                                                          on
                                                                              github_issues_timeline.search_key__number = pr_number.number
                                                       )
                                              WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                              'actor'),
                                                            'id') = 1
                                              )
                                     group by search_key__owner,
                                              search_key__repo,
                                              created_at,
                                              id
                                              ) b
                                 ON
                                             a.id = b.id

                                         and a.created_at = b.created_at
                                         and a.search_key__owner = b.search_key__owner
                                         and a.search_key__repo = b.search_key__repo) a
                            GLOBAL
                            FULL JOIN
                        (
                            --14 15
                            select if(a.search_key__owner != '',
                                      a.search_key__owner,
                                      b.search_key__owner) as owner,
                                   if(a.search_key__repo != '',
                                      a.search_key__repo,
                                      b.search_key__repo)  as repo,
                                   if(a.id != '',
                                      a.created_at,
                                      b.created_at)        as created_at,
                                   if(a.id != '',
                                      a.id,
                                      b.id)                as github_id,

                                   referred_other_issues_or_prs_in_issue,
                                   referred_other_issues_or_prs_in_pr
                            from (
                                     select cross_referenced.search_key__owner,
                                            cross_referenced.search_key__repo,
                                            created_at,
                                            id,

                                            COUNT(id) as referred_other_issues_or_prs_in_issue
                                     from (
                                              select search_key__owner,
                                                     search_key__repo,
                                                     toYYYYMM(toDate(substring(JSONExtractString(timeline_raw,
                                                                                        'created_at'), 1, 10)))

                                                                                as created_at,
                                                     JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'id')    as id
                                              from (select github_issues_timeline.*
                                                    from (select *,
                                                                 JSONExtractString(
                                                                         JSONExtractString(
                                                                                 JSONExtractString(timeline_raw,
                                                                                                   'source'),
                                                                                 'issue'),
                                                                         'number') as number
                                                          from github_issues_timeline
                                                          where search_key__owner = '{owner}'
                                                            and search_key__repo = '{repo}'
                                                            and search_key__event = 'cross-referenced') github_issues_timeline GLOBAL
                                                             JOIN
                                                         (
                                                             select DISTINCT `number`,
                                                                             search_key__owner,
                                                                             search_key__repo
                                                             from github_issues gict
                                                             WHERE pull_request__url = ''
                                                               and search_key__owner = '{owner}'
                                                               and search_key__repo = '{repo}') issues
                                                         on github_issues_timeline.search_key__owner =
                                                            issues.search_key__owner
                                                             and
                                                            github_issues_timeline.search_key__repo =
                                                            issues.search_key__repo
                                                             and
                                                            github_issues_timeline.number = toString(issues.number))

                                              WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                              'actor'),
                                                            'id') = 1) cross_referenced

                                     GROUP BY cross_referenced.search_key__owner,
                                              cross_referenced.search_key__repo,
                                              created_at,
                                              id
                                              ) a
                                     GLOBAL
                                     FULL JOIN
                                 (
                                     select cross_referenced.search_key__owner,
                                            cross_referenced.search_key__repo,
                                            created_at,
                                            id,

                                            COUNT(id) as referred_other_issues_or_prs_in_pr
                                     from (
                                              select search_key__owner,
                                                     search_key__repo,
                                                     toYYYYMM(toDate(substring(JSONExtractString(timeline_raw,
                                                                                        'created_at'), 1, 10)))

                                                                                as created_at,
                                                     JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'id')    as id

                                              from (select *
                                                    from (select *,
                                                                 JSONExtractString(
                                                                         JSONExtractString(
                                                                                 JSONExtractString(timeline_raw,
                                                                                                   'source'),
                                                                                 'issue'),
                                                                         'number') as number
                                                          from github_issues_timeline
                                                          where search_key__owner = '{owner}'
                                                            and search_key__repo = '{repo}'
                                                            and search_key__event = 'cross-referenced') github_issues_timeline GLOBAL
                                                             JOIN
                                                         (
                                                             select DISTINCT `number`,
                                                                             search_key__owner,
                                                                             search_key__repo
                                                             from github_issues gict
                                                             WHERE pull_request__url != ''
                                                               and search_key__owner = '{owner}'
                                                               and search_key__repo = '{repo}') prs
                                                         ON
                                                                     github_issues_timeline.search_key__owner =
                                                                     prs.search_key__owner
                                                                 and
                                                                     github_issues_timeline.search_key__repo =
                                                                     prs.search_key__repo
                                                                 and
                                                                     github_issues_timeline.number =
                                                                     toString(prs.number))
                                              WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                              'actor'),
                                                            'id') = 1) cross_referenced

                                     GROUP BY cross_referenced.search_key__owner,
                                              cross_referenced.search_key__repo,
                                              created_at,
                                              id
                                              ) b
                                 ON
                                             a.id = b.id

                                         and a.created_at = b.created_at
                                         and a.search_key__owner = b.search_key__owner
                                         and a.search_key__repo = b.search_key__repo) b
                        ON
                                    a.github_id = b.github_id

                                and a.created_at = b.created_at
                                and a.owner = b.owner
                                and a.repo = b.repo) a
                      global
                      full join
                  --16 17 18 19
                      (select if(a.owner != '',
                                 a.owner,
                                 b.owner)        as owner,
                              if(a.repo != '',
                                 a.repo,
                                 b.repo)         as repo,
                              if(a.github_id != '',
                                 a.created_at,
                                 b.created_at)   as created_at,
                              if(a.github_id != '',
                                 a.github_id,
                                 b.github_id)    as github_id,

                              changed_label_times_in_issues,
                              changed_label_times_in_prs,
                              closed_issues_times,
                              closed_prs_times
                       from (
                                --12 13
                                select if(a.search_key__owner != '',
                                          a.search_key__owner,
                                          b.search_key__owner) as owner,
                                       if(a.search_key__repo != '',
                                          a.search_key__repo,
                                          b.search_key__repo)  as repo,
                                       if(a.id != '',
                                          a.created_at,
                                          b.created_at)        as created_at,
                                       if(a.id != '',
                                          a.id,
                                          b.id)                as github_id,

                                       changed_label_times_in_issues,
                                       changed_label_times_in_prs
                                from (
                                         select github_issues_timeline.search_key__owner,
                                                github_issues_timeline.search_key__repo,
                                                toYYYYMM(toDate(substring(JSONExtractString(timeline_raw,
                                                                                   'created_at'), 1, 10)))

                                                                           as created_at,
                                                JSONExtractString(JSONExtractString(timeline_raw,
                                                                                    'actor'),
                                                                  'id')    as id,

                                                count(id)                  as changed_label_times_in_issues
                                         from (select *
                                               from github_issues_timeline
                                               where search_key__owner = '{owner}'
                                                 and search_key__repo = '{repo}'
                                                 and (search_key__event = 'labeled' or search_key__event = 'unlabeled')) github_issues_timeline
                                                  global
                                                  join
                                              (
                                                  select DISTINCT `number`,
                                                                  search_key__owner,
                                                                  search_key__repo
                                                  from github_issues gict
                                                  WHERE pull_request__url = ''
                                                    and search_key__owner = '{owner}'
                                                    and search_key__repo = '{repo}') as issues_number
                                              on
                                                          github_issues_timeline.search_key__number =
                                                          issues_number.number
                                                      and
                                                          github_issues_timeline.search_key__owner =
                                                          issues_number.search_key__owner
                                                      and
                                                          github_issues_timeline.search_key__repo =
                                                          issues_number.search_key__repo
                                         WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                         'actor'),
                                                       'id') = 1
                                         GROUP BY github_issues_timeline.search_key__owner,
                                                  github_issues_timeline.search_key__repo,
                                                  created_at,
                                                  id
                                                  ) a
                                         GLOBAL
                                         FULL JOIN
                                     (
                                         select github_issues_timeline.search_key__owner,
                                                github_issues_timeline.search_key__repo,
                                                toYYYYMM(toDate(substring(JSONExtractString(timeline_raw,
                                                                                   'created_at'), 1, 10)))

                                                                           as created_at,
                                                JSONExtractString(JSONExtractString(timeline_raw,
                                                                                    'actor'),
                                                                  'id')    as id,

                                                count(id)                  as changed_label_times_in_prs
                                         from (select *
                                               from github_issues_timeline
                                               where search_key__owner = '{owner}'
                                                 and search_key__repo = '{repo}'
                                                 and (search_key__event = 'labeled' or search_key__event = 'unlabeled')) github_issues_timeline
                                                  global
                                                  join
                                              (
                                                  select DISTINCT `number`,
                                                                  search_key__owner,
                                                                  search_key__repo
                                                  from github_issues gict
                                                  WHERE pull_request__url != ''
                                                    and search_key__owner = '{owner}'
                                                    and search_key__repo = '{repo}') as pr_number
                                              on
                                                          github_issues_timeline.search_key__number = pr_number.number
                                                      and
                                                          github_issues_timeline.search_key__owner =
                                                          pr_number.search_key__owner
                                                      and
                                                          github_issues_timeline.search_key__repo =
                                                          pr_number.search_key__repo
                                         WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                         'actor'),
                                                       'id') = 1
                                         GROUP BY github_issues_timeline.search_key__owner,
                                                  github_issues_timeline.search_key__repo,
                                                  created_at,
                                                  id) b
                                     ON
                                                 a.id = b.id
                                             and a.search_key__owner = b.search_key__owner
                                             and a.search_key__repo = b.search_key__repo
                                             and a.created_at = b.created_at) a
                                GLOBAL
                                FULL JOIN
                            (
                                --14 15
                                select if(a.search_key__owner != '',
                                          a.search_key__owner,
                                          b.search_key__owner) as owner,
                                       if(a.search_key__repo != '',
                                          a.search_key__repo,
                                          b.search_key__repo)  as repo,
                                       if(a.actor_id != '',
                                          a.created_at,
                                          b.created_at)        as created_at,
                                       if(a.actor_id != '',
                                          a.actor_id,
                                          b.actor_id)          as github_id,

                                       closed_issues_times,
                                       closed_prs_times
                                from (
                                         select search_key__owner,
                                                search_key__repo,
                                                toYYYYMM(toDate(substring(JSONExtractString(timeline_raw,
                                                                                   'created_at'), 1, 10)))

                                                                           as created_at,
                                                JSONExtractString(JSONExtractString(timeline_raw,
                                                                                    'actor'),
                                                                  'id')    as actor_id,

                                                COUNT()                       closed_issues_times
                                         from (
                                                  select github_issues_timeline.*
                                                  from (select *
                                                        from github_issues_timeline
                                                        where search_key__owner = '{owner}'
                                                          and search_key__repo = '{repo}'
                                                          and search_key__event = 'closed') github_issues_timeline global
                                                           join (
                                                      select DISTINCT `number`,
                                                                      search_key__owner,
                                                                      search_key__repo
                                                      from github_issues gict
                                                      WHERE pull_request__url = ''
                                                        and search_key__owner = '{owner}'
                                                        and search_key__repo = '{repo}') as issues_number
                                                                on
                                                                            github_issues_timeline.search_key__number =
                                                                            issues_number.number
                                                                        and github_issues_timeline.search_key__owner =
                                                                            issues_number.search_key__owner
                                                                        and github_issues_timeline.search_key__repo =
                                                                            issues_number.search_key__repo)
                                         WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                         'actor'),
                                                       'id') = 1
                                         group by search_key__owner,
                                                  search_key__repo,
                                                  created_at,
                                                  actor_id) a
                                         GLOBAL
                                         FULL JOIN
                                     (
                                         select search_key__owner,
                                                search_key__repo,
                                                toYYYYMM(toDate(substring(JSONExtractString(timeline_raw,
                                                                                   'created_at'), 1, 10)))

                                                                           as created_at,
                                                JSONExtractString(JSONExtractString(timeline_raw,
                                                                                    'actor'),
                                                                  'id')    as actor_id,

                                                COUNT()                       closed_prs_times
                                         from (
                                                  select github_issues_timeline.*
                                                  from (select *
                                                        from github_issues_timeline
                                                        where search_key__owner = '{owner}'
                                                          and search_key__repo = '{repo}'
                                                          and search_key__event = 'closed') github_issues_timeline global
                                                           join (
                                                      select DISTINCT `number`,
                                                                      search_key__owner,
                                                                      search_key__repo
                                                      from github_issues gict
                                                      WHERE pull_request__url != ''
                                                        and search_key__owner = '{owner}'
                                                        and search_key__repo = '{repo}') as pr_number
                                                                on
                                                                            github_issues_timeline.search_key__number =
                                                                            pr_number.number
                                                                        and github_issues_timeline.search_key__owner =
                                                                            pr_number.search_key__owner
                                                                        and github_issues_timeline.search_key__repo =
                                                                            pr_number.search_key__repo)
                                         WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                         'actor'),
                                                       'id') = 1
                                         group by search_key__owner,
                                                  search_key__repo,
                                                  created_at,
                                                  actor_id) b
                                     ON
                                                 a.actor_id = b.actor_id
                                             and a.created_at = b.created_at
                                             and a.search_key__owner = b.search_key__owner
                                             and a.search_key__repo = b.search_key__repo) b
                            ON
                                        a.github_id = b.github_id
                                    and a.created_at = b.created_at
                                    and a.owner = b.owner
                                    and a.repo = b.repo) b
                  on
                              a.github_id = b.github_id and
                              a.created_at = b.created_at) a
                 global
                 full join
             -- 4 5 6 7 8 9 10 11
                 (select if(a.owner != '',
                            a.owner,
                            b.owner)        as owner,
                         if(a.repo != '',
                            a.repo,
                            b.repo)         as repo,
                         if(a.github_id != 0,
                            a.created_at,
                            b.created_at)   as created_at,
                         if(a.github_id != 0,
                            a.github_id,
                            b.github_id)    as github_id,
                         user_prs_counts,
                         pr_body_length_avg,
                         pr_body_length_sum,
                         user_issues_counts,
                         issue_body_length_avg,
                         issue_body_length_sum,
                         issues_comment_count,
                         issues_comment_body_length_avg,
                         issues_comment_body_length_sum,
                         pr_comment_count,
                         pr_comment_body_length_avg,
                         pr_comment_body_length_sum

                  from
                      -- 4 5 6 7
                      (select if(a.search_key__owner != '',
                                 a.search_key__owner,
                                 b.search_key__owner) as owner,
                              if(a.search_key__repo != '',
                                 a.search_key__repo,
                                 b.search_key__repo)  as repo,
                              if(a.user__id != 0,
                                 a.created_at,
                                 b.created_at)        as created_at,
                              if(a.user__id != 0,
                                 a.user__id,
                                 b.user__id)          as github_id,
                              a.user_prs_counts,
                              a.body_length_avg       as pr_body_length_avg,
                              a.body_length_sum       as pr_body_length_sum,
                              b.user_issues_counts,
                              b.body_length_avg       as issue_body_length_avg,
                              b.body_length_sum       as issue_body_length_sum
                       from (
                                --4,5
                                select `search_key__owner`,
                                       `search_key__repo`,
                                       toYYYYMM(toDate(created_at))
                                           created_at,
                                       `user__id`,
                                       COUNT(user__id) as    `user_prs_counts`,
                                       sum(lengthUTF8(body)) body_length_sum,
                                       avg(lengthUTF8(body)) `body_length_avg`
                                from github_pull_requests
                                where search_key__owner = '{owner}'
                                  and search_key__repo = '{repo}'
                                group by `search_key__owner`,
                                         `search_key__repo`,
                                         created_at,
                                         `user__id`) a
                                GLOBAL
                                FULL JOIN
                            (
                                -- 6 7
                                select `search_key__owner`,
                                       `search_key__repo`,
                                       toYYYYMM(toDate(created_at))
                                           created_at,
                                       `user__id`,
                                       COUNT(user__id) as    `user_issues_counts`,
                                       sum(lengthUTF8(body)) body_length_sum,
                                       avg(lengthUTF8(body)) `body_length_avg`
                                from github_issues
                                where pull_request__url == ''
                                  and search_key__owner = '{owner}'
                                  and search_key__repo = '{repo}'
                                group by `search_key__owner`,
                                         `search_key__repo`,
                                         created_at,
                                         `user__id`) b
                            ON
                                        a.user__id = b.user__id
                                    and a.created_at = b.created_at
                                    and a.search_key__owner = b.search_key__owner
                                    and a.search_key__repo = b.search_key__repo) a
                          global
                          full join
                      -- 8 9 10 11
                          (select if(a.search_key__owner != '',
                                     a.search_key__owner,
                                     b.search_key__owner) as owner,
                                  if(a.search_key__repo != '',
                                     a.search_key__repo,
                                     b.search_key__repo)  as repo,
                                  if(a.user__id != 0,
                                     a.created_at,
                                     b.created_at)        as created_at,
                                  if(a.user__id != 0,
                                     a.user__id,
                                     b.user__id)          as github_id,

                                  a.issues_comment_count,
                                  a.issues_comment_body_length_avg,
                                  a.body_length_sum       as issues_comment_body_length_sum,
                                  b.pr_comment_count,
                                  b.pr_comment_body_length_avg,
                                  b.body_length_sum       as pr_comment_body_length_sum
                           from (
                                    select search_key__owner,
                                           search_key__repo,
                                           toYYYYMM(toDate(created_at))
                                               created_at,
                                           user__id,

                                           COUNT() as            issues_comment_count,
                                           sum(lengthUTF8(body)) body_length_sum,
                                           avg(lengthUTF8(body)) issues_comment_body_length_avg
                                    from (
                                             select github_issues_comments.*
                                             from (select *
                                                   from github_issues_comments
                                                   where search_key__owner = '{owner}'
                                                     and search_key__repo = '{repo}') github_issues_comments global
                                                      semi
                                                      left join (
                                                 select DISTINCT `number`,
                                                                 search_key__owner,
                                                                 search_key__repo
                                                 from github_issues gict
                                                 WHERE pull_request__url = ''
                                                   and search_key__owner = '{owner}'
                                                   and search_key__repo = '{repo}') as pr_number
                                                                on
                                                                    github_issues_comments.search_key__number = pr_number.number
                                             )
                                    group by search_key__owner,
                                             search_key__repo,
                                             created_at,
                                             user__id) a
                                    GLOBAL
                                    FULL JOIN
                                (
                                    select search_key__owner,
                                           search_key__repo,
                                           toYYYYMM(toDate(created_at))
                                               created_at,
                                           user__id,
                                           COUNT() as            pr_comment_count,
                                           sum(lengthUTF8(body)) body_length_sum,
                                           avg(lengthUTF8(body)) pr_comment_body_length_avg
                                    from (
                                             select github_issues_comments.*
                                             from (select *
                                                   from github_issues_comments
                                                   where search_key__owner = '{owner}'
                                                     and search_key__repo = '{repo}') github_issues_comments global
                                                      semi
                                                      left join (
                                                 select DISTINCT `number`,
                                                                 search_key__owner,
                                                                 search_key__repo
                                                 from github_issues gict
                                                 WHERE pull_request__url != ''
                                                   and search_key__owner = '{owner}'
                                                   and search_key__repo = '{repo}') as pr_number
                                                                on
                                                                    github_issues_comments.search_key__number = pr_number.number
                                             )
                                    group by search_key__owner,
                                             search_key__repo,
                                             created_at,
                                             user__id) b
                                ON
                                            a.user__id = b.user__id and
                                            a.created_at = b.created_at
                                        and a.search_key__owner = b.search_key__owner
                                        and a.search_key__repo = b.search_key__repo) b
                      on
                                  a.github_id = b.github_id and
                                  a.created_at = b.created_at) b
             on
                         toInt64(a.github_id) = b.github_id and

                         a.created_at = b.created_at) b
    on
                a.github_id = b.github_id and

                a.commite_date = b.created_at;
"""
    # table_name = 'metrics_day_timeline'
    insert_sql = f"insert into {table_name} values"
    logger.info(f"计算owner: {owner},repo: {repo}        metrics......")
    results = ck.execute_no_params(sql)
    bulk_data = []
    count = 0
    for result in results:
        count += 1
        data_dict = {}
        data_dict["ck_data_insert_at"] = int(time.time() * 1000)
        data_dict["owner"] = result[0]
        data_dict["repo"] = result[1]
        data_dict["created_at"] = datetime.combine(result[2], datetime.min.time())
        data_dict["github_id"] = result[3]
        # data_dict["github_login"] = result[4]
        # data_dict["git_author_email"] = result[5]
        data_dict["commit_times"] = result[4]
        data_dict["changed_lines"] = result[5]
        data_dict["diff_file_counts"] = result[6]
        data_dict["prs_counts"] = result[7]
        data_dict["pr_body_length_avg"] = result[8]
        data_dict["pr_body_length_sum"] = result[9]
        data_dict["issues_counts"] = result[10]
        data_dict["issue_body_length_avg"] = result[11]
        data_dict["issue_body_length_sum"] = result[12]
        data_dict["issues_comment_count"] = result[13]
        data_dict["issues_comment_body_length_avg"] = result[14]
        data_dict["issues_comment_body_length_sum"] = result[15]
        data_dict["pr_comment_count"] = result[16]
        data_dict["pr_comment_body_length_avg"] = result[17]
        data_dict["pr_comment_body_length_sum"] = result[18]
        data_dict["be_mentioned_times_in_issues"] = result[19]
        data_dict["be_mentioned_times_in_pr"] = result[20]
        data_dict["referred_other_issues_or_prs_in_issue"] = result[21]
        data_dict["referred_other_issues_or_prs_in_pr"] = result[22]
        data_dict["changed_label_times_in_issues"] = result[23]
        data_dict["changed_label_times_in_prs"] = result[24]
        data_dict["closed_issues_times"] = result[25]
        data_dict["closed_prs_times"] = result[26]

        bulk_data.append(data_dict)

        if len(bulk_data) == 20000:
            ck.execute(sql=insert_sql, params=bulk_data)
            logger.info(f"已经插入{count}条")
            bulk_data.clear()
    if bulk_data:
        ck.execute(sql=insert_sql, params=bulk_data)
        logger.info(f"已经插入{count}条")
    logger.info("wait 30 sec........")
    time.sleep(30)
    if not if_data_eq_github(count=count, ck=ck, table_name=table_name, owner=owner, repo=repo):
        raise Exception("Insert failed：Inconsistent data count")
    else:
        logger.info("Successfully inserted: datas are consistent")
    ck.close()


def get_metries_year_timeline_by_repo(ck, owner="", repo="", table_name=""):
    # owner = 'kubernetes'
    # repo = 'kubernetes'
    sql = f"""select 
    if(a.owner != '',
          a.owner,
          b.owner)        as owner,
       if(a.repo != '',
          a.repo,
          b.repo)         as repo,
       if(a.github_id != 0,
          a.commite_date,
          b.created_at)   as created_at,
       if(a.github_id != 0,
          a.github_id,
          b.github_id)    as github_id,

       commit_times,
       all_lines,
       file_counts,
       user_prs_counts,
       pr_body_length_avg,
       pr_body_length_sum,
       user_issues_counts,
       issue_body_length_avg,
       issue_body_length_sum,
       issues_comment_count,
       issues_comment_body_length_avg,
       issues_comment_body_length_sum,
       pr_comment_count,
       pr_comment_body_length_avg,
       pr_comment_body_length_sum,
       be_mentioned_times_in_issues,
       be_mentioned_times_in_pr,
       referred_other_issues_or_prs_in_issue,
       referred_other_issues_or_prs_in_pr,
       changed_label_times_in_issues,
       changed_label_times_in_prs,
       closed_issues_times,
       closed_prs_times
from
    --1 2 3
    (select owner,
            repo,
            commite_date,
            github_id,
            sum(commit_times) as commit_times,
            sum(all_lines)    as all_lines,
            sum(file_counts)  as file_counts
     from (select a.search_key__owner as owner,
                  a.search_key__repo  as repo,
                  a.commite_date,
                  b.author__id        as github_id,
                  author_email        as git_author_email,
                  commit_times,
                  all_lines,
                  file_counts
           from (
                    --按照邮箱项目分组统计提交代码次数和更改代码行数更改过多少的不同的文件
                    select commits_all_lines.*,
                           changed_files.file_counts
                    from (
                             -- 按照邮箱项目分组统计提交代码次数和更改代码行数
                             select search_key__owner,
                                    search_key__repo,
                                    author_email,
                                    toYear(toDate(committed_date))
                                     commite_date,
                                    COUNT(author_email) as commit_times,
                                    SUM(total__lines)   as all_lines
                             from gits
                             where search_key__owner = '{owner}'
                               and search_key__repo = '{repo}'
                               and author_email != ''
                             group by search_key__owner,
                                      search_key__repo,
                                      commite_date,
                                      author_email
                             ) commits_all_lines
                             GLOBAL
                             full JOIN
                         (
                             --统计这个人更改过多少的不同的文件
                             select search_key__owner,
                                    search_key__repo,
                                    commite_date,
                                    author_email,
                                    COUNT(*) file_counts
                             from (
                                      select DISTINCT `search_key__owner`,
                                                      `search_key__repo`,
                                                      `author_email`,
                                                      toYear( toDate(committed_date))
                                                      commite_date,
                                                      `files.file_name` as   file_name
                                      from `gits`
                                               array join `files.file_name`
                                      where search_key__owner = '{owner}'
                                        and search_key__repo = '{repo}'
                                        and author_email != '')
                             group by search_key__owner,
                                      search_key__repo,
                                      commite_date,
                                      author_email) changed_files
                         ON
                                     commits_all_lines.search_key__owner = changed_files.search_key__owner
                                 AND
                                     commits_all_lines.search_key__repo = changed_files.search_key__repo
                                 AND
                                     commits_all_lines.author_email = changed_files.author_email
                                 and commits_all_lines.commite_date = changed_files.commite_date) a
                    GLOBAL
                    JOIN
                (
                    select DISTINCT search_key__owner,
                                    search_key__repo,
                                    commit__author__email,
                                    author__id
                    from github_commits gct
                    where author__id != 0
                      and search_key__owner = '{owner}'
                      and search_key__repo = '{repo}') b
                on
                            a.search_key__owner = b.search_key__owner
                        and a.search_key__repo = b.search_key__repo
                        and a.author_email = b.commit__author__email)
     group by owner, repo, commite_date, github_id
        ) a
        global
        full join
    -- 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19
        (select if(a.owner != '',
                   a.owner,
                   b.owner)        as owner,
                if(a.repo != '',
                   a.repo,
                   b.repo)         as repo,
                if(a.github_id != '',
                   a.created_at,
                   b.created_at)   as created_at,
                if(a.github_id != '',
                   toInt64(a.github_id),
                   b.github_id)    as github_id,

                user_prs_counts,
                pr_body_length_avg,
                pr_body_length_sum,
                user_issues_counts,
                issue_body_length_avg,
                issue_body_length_sum,
                issues_comment_count,
                issues_comment_body_length_avg,
                issues_comment_body_length_sum,
                pr_comment_count,
                pr_comment_body_length_avg,
                pr_comment_body_length_sum,
                be_mentioned_times_in_issues,
                be_mentioned_times_in_pr,
                referred_other_issues_or_prs_in_issue,
                referred_other_issues_or_prs_in_pr,
                changed_label_times_in_issues,
                changed_label_times_in_prs,
                closed_issues_times,
                closed_prs_times
         from
             --12 13 14 15 16 17 18 19
             (select if(a.owner != '',
                        a.owner,
                        b.owner)        as owner,
                     if(a.repo != '',
                        a.repo,
                        b.repo)         as repo,
                     if(a.github_id != '',
                        a.created_at,
                        b.created_at)   as created_at,
                     if(a.github_id != '',
                        a.github_id,
                        b.github_id)    as github_id,

                     be_mentioned_times_in_issues,
                     be_mentioned_times_in_pr,
                     referred_other_issues_or_prs_in_issue,
                     referred_other_issues_or_prs_in_pr,
                     changed_label_times_in_issues,
                     changed_label_times_in_prs,
                     closed_issues_times,
                     closed_prs_times

              from
                  --12 13 14 15
                  (select if(a.owner != '',
                             a.owner,
                             b.owner)        as owner,
                          if(a.repo != '',
                             a.repo,
                             b.repo)         as repo,
                          if(a.github_id != '',
                             a.created_at,
                             b.created_at)   as created_at,
                          if(a.github_id != '',
                             a.github_id,
                             b.github_id)    as github_id,

                          be_mentioned_times_in_issues,
                          be_mentioned_times_in_pr,
                          referred_other_issues_or_prs_in_issue,
                          referred_other_issues_or_prs_in_pr
                   from (
                            --12,13
                            select if(a.search_key__owner != '',
                                      a.search_key__owner,
                                      b.search_key__owner) as owner,
                                   if(a.search_key__repo != '',
                                      a.search_key__repo,
                                      b.search_key__repo)  as repo,
                                   if(a.id != '',
                                      a.created_at,
                                      b.created_at)        as created_at,
                                   if(a.id != '',
                                      a.id,
                                      b.id)                as github_id,

                                   be_mentioned_times_in_issues,
                                   be_mentioned_times_in_pr
                            from (
                                     select search_key__owner,
                                            search_key__repo,
                                            created_at,
                                            id,

                                            COUNT() as be_mentioned_times_in_issues
                                     from (
                                              select search_key__owner,
                                                     search_key__repo,
                                                     toYear(toDate(substring(JSONExtractString(timeline_raw,
                                                                                        'created_at'), 1, 10)))

                                                                                as created_at,
                                                     JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'id')    as id

                                              from (
                                                       select github_issues_timeline.*
                                                       from (select *
                                                             from github_issues_timeline
                                                             where search_key__owner = '{owner}'
                                                               and search_key__repo = '{repo}'
                                                               and search_key__event = 'mentioned') github_issues_timeline global semi
                                                                left join (
                                                           select DISTINCT `number`,
                                                                           search_key__owner,
                                                                           search_key__repo
                                                           from github_issues gict
                                                           WHERE pull_request__url = ''
                                                             and search_key__owner = '{owner}'
                                                             and search_key__repo = '{repo}') as issues_number
                                                                          on
                                                                              github_issues_timeline.search_key__number = issues_number.number
                                                       )
                                              WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                              'actor'),
                                                            'id') = 1
                                              )
                                     group by search_key__owner,
                                              search_key__repo,
                                              created_at,
                                              id
                                              ) a
                                     GLOBAL
                                     FULL JOIN
                                 (
                                     select search_key__owner,
                                            search_key__repo,
                                            created_at,
                                            id,

                                            COUNT() as be_mentioned_times_in_pr
                                     from (
                                              select search_key__owner,
                                                     search_key__repo,
                                                     toYear(toDate(substring(JSONExtractString(timeline_raw,
                                                                                        'created_at'), 1, 10)))

                                                                                as created_at,
                                                     JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'id')    as id
                                              from (
                                                       select github_issues_timeline.*
                                                       from (select *
                                                             from github_issues_timeline
                                                             where search_key__owner = '{owner}'
                                                               and search_key__repo = '{repo}'
                                                               and search_key__event = 'mentioned') github_issues_timeline global semi
                                                                left join (
                                                           select DISTINCT `number`,
                                                                           search_key__owner,
                                                                           search_key__repo
                                                           from github_issues gict
                                                           WHERE pull_request__url != ''
                                                             and search_key__owner = '{owner}'
                                                             and search_key__repo = '{repo}') as pr_number
                                                                          on
                                                                              github_issues_timeline.search_key__number = pr_number.number
                                                       )
                                              WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                              'actor'),
                                                            'id') = 1
                                              )
                                     group by search_key__owner,
                                              search_key__repo,
                                              created_at,
                                              id
                                              ) b
                                 ON
                                             a.id = b.id

                                         and a.created_at = b.created_at
                                         and a.search_key__owner = b.search_key__owner
                                         and a.search_key__repo = b.search_key__repo) a
                            GLOBAL
                            FULL JOIN
                        (
                            --14 15
                            select if(a.search_key__owner != '',
                                      a.search_key__owner,
                                      b.search_key__owner) as owner,
                                   if(a.search_key__repo != '',
                                      a.search_key__repo,
                                      b.search_key__repo)  as repo,
                                   if(a.id != '',
                                      a.created_at,
                                      b.created_at)        as created_at,
                                   if(a.id != '',
                                      a.id,
                                      b.id)                as github_id,

                                   referred_other_issues_or_prs_in_issue,
                                   referred_other_issues_or_prs_in_pr
                            from (
                                     select cross_referenced.search_key__owner,
                                            cross_referenced.search_key__repo,
                                            created_at,
                                            id,

                                            COUNT(id) as referred_other_issues_or_prs_in_issue
                                     from (
                                              select search_key__owner,
                                                     search_key__repo,
                                                     toYear(toDate(substring(JSONExtractString(timeline_raw,
                                                                                        'created_at'), 1, 10)))

                                                                                as created_at,
                                                     JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'id')    as id
                                              from (select github_issues_timeline.*
                                                    from (select *,
                                                                 JSONExtractString(
                                                                         JSONExtractString(
                                                                                 JSONExtractString(timeline_raw,
                                                                                                   'source'),
                                                                                 'issue'),
                                                                         'number') as number
                                                          from github_issues_timeline
                                                          where search_key__owner = '{owner}'
                                                            and search_key__repo = '{repo}'
                                                            and search_key__event = 'cross-referenced') github_issues_timeline GLOBAL
                                                             JOIN
                                                         (
                                                             select DISTINCT `number`,
                                                                             search_key__owner,
                                                                             search_key__repo
                                                             from github_issues gict
                                                             WHERE pull_request__url = ''
                                                               and search_key__owner = '{owner}'
                                                               and search_key__repo = '{repo}') issues
                                                         on github_issues_timeline.search_key__owner =
                                                            issues.search_key__owner
                                                             and
                                                            github_issues_timeline.search_key__repo =
                                                            issues.search_key__repo
                                                             and
                                                            github_issues_timeline.number = toString(issues.number))

                                              WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                              'actor'),
                                                            'id') = 1) cross_referenced

                                     GROUP BY cross_referenced.search_key__owner,
                                              cross_referenced.search_key__repo,
                                              created_at,
                                              id
                                              ) a
                                     GLOBAL
                                     FULL JOIN
                                 (
                                     select cross_referenced.search_key__owner,
                                            cross_referenced.search_key__repo,
                                            created_at,
                                            id,

                                            COUNT(id) as referred_other_issues_or_prs_in_pr
                                     from (
                                              select search_key__owner,
                                                     search_key__repo,
                                                     toYear(toDate(substring(JSONExtractString(timeline_raw,
                                                                                        'created_at'), 1, 10)))

                                                                                as created_at,
                                                     JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'id')    as id

                                              from (select *
                                                    from (select *,
                                                                 JSONExtractString(
                                                                         JSONExtractString(
                                                                                 JSONExtractString(timeline_raw,
                                                                                                   'source'),
                                                                                 'issue'),
                                                                         'number') as number
                                                          from github_issues_timeline
                                                          where search_key__owner = '{owner}'
                                                            and search_key__repo = '{repo}'
                                                            and search_key__event = 'cross-referenced') github_issues_timeline GLOBAL
                                                             JOIN
                                                         (
                                                             select DISTINCT `number`,
                                                                             search_key__owner,
                                                                             search_key__repo
                                                             from github_issues gict
                                                             WHERE pull_request__url != ''
                                                               and search_key__owner = '{owner}'
                                                               and search_key__repo = '{repo}') prs
                                                         ON
                                                                     github_issues_timeline.search_key__owner =
                                                                     prs.search_key__owner
                                                                 and
                                                                     github_issues_timeline.search_key__repo =
                                                                     prs.search_key__repo
                                                                 and
                                                                     github_issues_timeline.number =
                                                                     toString(prs.number))
                                              WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                              'actor'),
                                                            'id') = 1) cross_referenced

                                     GROUP BY cross_referenced.search_key__owner,
                                              cross_referenced.search_key__repo,
                                              created_at,
                                              id
                                              ) b
                                 ON
                                             a.id = b.id

                                         and a.created_at = b.created_at
                                         and a.search_key__owner = b.search_key__owner
                                         and a.search_key__repo = b.search_key__repo) b
                        ON
                                    a.github_id = b.github_id

                                and a.created_at = b.created_at
                                and a.owner = b.owner
                                and a.repo = b.repo) a
                      global
                      full join
                  --16 17 18 19
                      (select if(a.owner != '',
                                 a.owner,
                                 b.owner)        as owner,
                              if(a.repo != '',
                                 a.repo,
                                 b.repo)         as repo,
                              if(a.github_id != '',
                                 a.created_at,
                                 b.created_at)   as created_at,
                              if(a.github_id != '',
                                 a.github_id,
                                 b.github_id)    as github_id,

                              changed_label_times_in_issues,
                              changed_label_times_in_prs,
                              closed_issues_times,
                              closed_prs_times
                       from (
                                --12 13
                                select if(a.search_key__owner != '',
                                          a.search_key__owner,
                                          b.search_key__owner) as owner,
                                       if(a.search_key__repo != '',
                                          a.search_key__repo,
                                          b.search_key__repo)  as repo,
                                       if(a.id != '',
                                          a.created_at,
                                          b.created_at)        as created_at,
                                       if(a.id != '',
                                          a.id,
                                          b.id)                as github_id,

                                       changed_label_times_in_issues,
                                       changed_label_times_in_prs
                                from (
                                         select github_issues_timeline.search_key__owner,
                                                github_issues_timeline.search_key__repo,
                                                toYear(toDate(substring(JSONExtractString(timeline_raw,
                                                                                   'created_at'), 1, 10)))

                                                                           as created_at,
                                                JSONExtractString(JSONExtractString(timeline_raw,
                                                                                    'actor'),
                                                                  'id')    as id,

                                                count(id)                  as changed_label_times_in_issues
                                         from (select *
                                               from github_issues_timeline
                                               where search_key__owner = '{owner}'
                                                 and search_key__repo = '{repo}'
                                                 and (search_key__event = 'labeled' or search_key__event = 'unlabeled')) github_issues_timeline
                                                  global
                                                  join
                                              (
                                                  select DISTINCT `number`,
                                                                  search_key__owner,
                                                                  search_key__repo
                                                  from github_issues gict
                                                  WHERE pull_request__url = ''
                                                    and search_key__owner = '{owner}'
                                                    and search_key__repo = '{repo}') as issues_number
                                              on
                                                          github_issues_timeline.search_key__number =
                                                          issues_number.number
                                                      and
                                                          github_issues_timeline.search_key__owner =
                                                          issues_number.search_key__owner
                                                      and
                                                          github_issues_timeline.search_key__repo =
                                                          issues_number.search_key__repo
                                         WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                         'actor'),
                                                       'id') = 1
                                         GROUP BY github_issues_timeline.search_key__owner,
                                                  github_issues_timeline.search_key__repo,
                                                  created_at,
                                                  id
                                                  ) a
                                         GLOBAL
                                         FULL JOIN
                                     (
                                         select github_issues_timeline.search_key__owner,
                                                github_issues_timeline.search_key__repo,
                                                toYear(toDate(substring(JSONExtractString(timeline_raw,
                                                                                   'created_at'), 1, 10)))

                                                                           as created_at,
                                                JSONExtractString(JSONExtractString(timeline_raw,
                                                                                    'actor'),
                                                                  'id')    as id,

                                                count(id)                  as changed_label_times_in_prs
                                         from (select *
                                               from github_issues_timeline
                                               where search_key__owner = '{owner}'
                                                 and search_key__repo = '{repo}'
                                                 and (search_key__event = 'labeled' or search_key__event = 'unlabeled')) github_issues_timeline
                                                  global
                                                  join
                                              (
                                                  select DISTINCT `number`,
                                                                  search_key__owner,
                                                                  search_key__repo
                                                  from github_issues gict
                                                  WHERE pull_request__url != ''
                                                    and search_key__owner = '{owner}'
                                                    and search_key__repo = '{repo}') as pr_number
                                              on
                                                          github_issues_timeline.search_key__number = pr_number.number
                                                      and
                                                          github_issues_timeline.search_key__owner =
                                                          pr_number.search_key__owner
                                                      and
                                                          github_issues_timeline.search_key__repo =
                                                          pr_number.search_key__repo
                                         WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                         'actor'),
                                                       'id') = 1
                                         GROUP BY github_issues_timeline.search_key__owner,
                                                  github_issues_timeline.search_key__repo,
                                                  created_at,
                                                  id) b
                                     ON
                                                 a.id = b.id
                                             and a.search_key__owner = b.search_key__owner
                                             and a.search_key__repo = b.search_key__repo
                                             and a.created_at = b.created_at) a
                                GLOBAL
                                FULL JOIN
                            (
                                --14 15
                                select if(a.search_key__owner != '',
                                          a.search_key__owner,
                                          b.search_key__owner) as owner,
                                       if(a.search_key__repo != '',
                                          a.search_key__repo,
                                          b.search_key__repo)  as repo,
                                       if(a.actor_id != '',
                                          a.created_at,
                                          b.created_at)        as created_at,
                                       if(a.actor_id != '',
                                          a.actor_id,
                                          b.actor_id)          as github_id,

                                       closed_issues_times,
                                       closed_prs_times
                                from (
                                         select search_key__owner,
                                                search_key__repo,
                                                toYear(toDate(substring(JSONExtractString(timeline_raw,
                                                                                   'created_at'), 1, 10)))

                                                                           as created_at,
                                                JSONExtractString(JSONExtractString(timeline_raw,
                                                                                    'actor'),
                                                                  'id')    as actor_id,

                                                COUNT()                       closed_issues_times
                                         from (
                                                  select github_issues_timeline.*
                                                  from (select *
                                                        from github_issues_timeline
                                                        where search_key__owner = '{owner}'
                                                          and search_key__repo = '{repo}'
                                                          and search_key__event = 'closed') github_issues_timeline global
                                                           join (
                                                      select DISTINCT `number`,
                                                                      search_key__owner,
                                                                      search_key__repo
                                                      from github_issues gict
                                                      WHERE pull_request__url = ''
                                                        and search_key__owner = '{owner}'
                                                        and search_key__repo = '{repo}') as issues_number
                                                                on
                                                                            github_issues_timeline.search_key__number =
                                                                            issues_number.number
                                                                        and github_issues_timeline.search_key__owner =
                                                                            issues_number.search_key__owner
                                                                        and github_issues_timeline.search_key__repo =
                                                                            issues_number.search_key__repo)
                                         WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                         'actor'),
                                                       'id') = 1
                                         group by search_key__owner,
                                                  search_key__repo,
                                                  created_at,
                                                  actor_id) a
                                         GLOBAL
                                         FULL JOIN
                                     (
                                         select search_key__owner,
                                                search_key__repo,
                                                toYear(toDate(substring(JSONExtractString(timeline_raw,
                                                                                   'created_at'), 1, 10)))

                                                                           as created_at,
                                                JSONExtractString(JSONExtractString(timeline_raw,
                                                                                    'actor'),
                                                                  'id')    as actor_id,

                                                COUNT()                       closed_prs_times
                                         from (
                                                  select github_issues_timeline.*
                                                  from (select *
                                                        from github_issues_timeline
                                                        where search_key__owner = '{owner}'
                                                          and search_key__repo = '{repo}'
                                                          and search_key__event = 'closed') github_issues_timeline global
                                                           join (
                                                      select DISTINCT `number`,
                                                                      search_key__owner,
                                                                      search_key__repo
                                                      from github_issues gict
                                                      WHERE pull_request__url != ''
                                                        and search_key__owner = '{owner}'
                                                        and search_key__repo = '{repo}') as pr_number
                                                                on
                                                                            github_issues_timeline.search_key__number =
                                                                            pr_number.number
                                                                        and github_issues_timeline.search_key__owner =
                                                                            pr_number.search_key__owner
                                                                        and github_issues_timeline.search_key__repo =
                                                                            pr_number.search_key__repo)
                                         WHERE JSONHas(JSONExtractString(timeline_raw,
                                                                         'actor'),
                                                       'id') = 1
                                         group by search_key__owner,
                                                  search_key__repo,
                                                  created_at,
                                                  actor_id) b
                                     ON
                                                 a.actor_id = b.actor_id
                                             and a.created_at = b.created_at
                                             and a.search_key__owner = b.search_key__owner
                                             and a.search_key__repo = b.search_key__repo) b
                            ON
                                        a.github_id = b.github_id
                                    and a.created_at = b.created_at
                                    and a.owner = b.owner
                                    and a.repo = b.repo) b
                  on
                              a.github_id = b.github_id and
                              a.created_at = b.created_at) a
                 global
                 full join
             -- 4 5 6 7 8 9 10 11
                 (select if(a.owner != '',
                            a.owner,
                            b.owner)        as owner,
                         if(a.repo != '',
                            a.repo,
                            b.repo)         as repo,
                         if(a.github_id != 0,
                            a.created_at,
                            b.created_at)   as created_at,
                         if(a.github_id != 0,
                            a.github_id,
                            b.github_id)    as github_id,
                         user_prs_counts,
                         pr_body_length_avg,
                         pr_body_length_sum,
                         user_issues_counts,
                         issue_body_length_avg,
                         issue_body_length_sum,
                         issues_comment_count,
                         issues_comment_body_length_avg,
                         issues_comment_body_length_sum,
                         pr_comment_count,
                         pr_comment_body_length_avg,
                         pr_comment_body_length_sum

                  from
                      -- 4 5 6 7
                      (select if(a.search_key__owner != '',
                                 a.search_key__owner,
                                 b.search_key__owner) as owner,
                              if(a.search_key__repo != '',
                                 a.search_key__repo,
                                 b.search_key__repo)  as repo,
                              if(a.user__id != 0,
                                 a.created_at,
                                 b.created_at)        as created_at,
                              if(a.user__id != 0,
                                 a.user__id,
                                 b.user__id)          as github_id,
                              a.user_prs_counts,
                              a.body_length_avg       as pr_body_length_avg,
                              a.body_length_sum       as pr_body_length_sum,
                              b.user_issues_counts,
                              b.body_length_avg       as issue_body_length_avg,
                              b.body_length_sum       as issue_body_length_sum
                       from (
                                --4,5
                                select `search_key__owner`,
                                       `search_key__repo`,
                                       toYear(toDate(created_at))
                                           created_at,
                                       `user__id`,
                                       COUNT(user__id) as    `user_prs_counts`,
                                       sum(lengthUTF8(body)) body_length_sum,
                                       avg(lengthUTF8(body)) `body_length_avg`
                                from github_pull_requests
                                where search_key__owner = '{owner}'
                                  and search_key__repo = '{repo}'
                                group by `search_key__owner`,
                                         `search_key__repo`,
                                         created_at,
                                         `user__id`) a
                                GLOBAL
                                FULL JOIN
                            (
                                -- 6 7
                                select `search_key__owner`,
                                       `search_key__repo`,
                                       toYear(toDate(created_at))
                                           created_at,
                                       `user__id`,
                                       COUNT(user__id) as    `user_issues_counts`,
                                       sum(lengthUTF8(body)) body_length_sum,
                                       avg(lengthUTF8(body)) `body_length_avg`
                                from github_issues
                                where pull_request__url == ''
                                  and search_key__owner = '{owner}'
                                  and search_key__repo = '{repo}'
                                group by `search_key__owner`,
                                         `search_key__repo`,
                                         created_at,
                                         `user__id`) b
                            ON
                                        a.user__id = b.user__id
                                    and a.created_at = b.created_at
                                    and a.search_key__owner = b.search_key__owner
                                    and a.search_key__repo = b.search_key__repo) a
                          global
                          full join
                      -- 8 9 10 11
                          (select if(a.search_key__owner != '',
                                     a.search_key__owner,
                                     b.search_key__owner) as owner,
                                  if(a.search_key__repo != '',
                                     a.search_key__repo,
                                     b.search_key__repo)  as repo,
                                  if(a.user__id != 0,
                                     a.created_at,
                                     b.created_at)        as created_at,
                                  if(a.user__id != 0,
                                     a.user__id,
                                     b.user__id)          as github_id,

                                  a.issues_comment_count,
                                  a.issues_comment_body_length_avg,
                                  a.body_length_sum       as issues_comment_body_length_sum,
                                  b.pr_comment_count,
                                  b.pr_comment_body_length_avg,
                                  b.body_length_sum       as pr_comment_body_length_sum
                           from (
                                    select search_key__owner,
                                           search_key__repo,
                                           toYear(toDate(created_at))
                                               created_at,
                                           user__id,

                                           COUNT() as            issues_comment_count,
                                           sum(lengthUTF8(body)) body_length_sum,
                                           avg(lengthUTF8(body)) issues_comment_body_length_avg
                                    from (
                                             select github_issues_comments.*
                                             from (select *
                                                   from github_issues_comments
                                                   where search_key__owner = '{owner}'
                                                     and search_key__repo = '{repo}') github_issues_comments global
                                                      semi
                                                      left join (
                                                 select DISTINCT `number`,
                                                                 search_key__owner,
                                                                 search_key__repo
                                                 from github_issues gict
                                                 WHERE pull_request__url = ''
                                                   and search_key__owner = '{owner}'
                                                   and search_key__repo = '{repo}') as pr_number
                                                                on
                                                                    github_issues_comments.search_key__number = pr_number.number
                                             )
                                    group by search_key__owner,
                                             search_key__repo,
                                             created_at,
                                             user__id) a
                                    GLOBAL
                                    FULL JOIN
                                (
                                    select search_key__owner,
                                           search_key__repo,
                                           toYear(toDate(created_at))
                                               created_at,
                                           user__id,
                                           COUNT() as            pr_comment_count,
                                           sum(lengthUTF8(body)) body_length_sum,
                                           avg(lengthUTF8(body)) pr_comment_body_length_avg
                                    from (
                                             select github_issues_comments.*
                                             from (select *
                                                   from github_issues_comments
                                                   where search_key__owner = '{owner}'
                                                     and search_key__repo = '{repo}') github_issues_comments global
                                                      semi
                                                      left join (
                                                 select DISTINCT `number`,
                                                                 search_key__owner,
                                                                 search_key__repo
                                                 from github_issues gict
                                                 WHERE pull_request__url != ''
                                                   and search_key__owner = '{owner}'
                                                   and search_key__repo = '{repo}') as pr_number
                                                                on
                                                                    github_issues_comments.search_key__number = pr_number.number
                                             )
                                    group by search_key__owner,
                                             search_key__repo,
                                             created_at,
                                             user__id) b
                                ON
                                            a.user__id = b.user__id and
                                            a.created_at = b.created_at
                                        and a.search_key__owner = b.search_key__owner
                                        and a.search_key__repo = b.search_key__repo) b
                      on
                                  a.github_id = b.github_id and
                                  a.created_at = b.created_at) b
             on
                         toInt64(a.github_id) = b.github_id and

                         a.created_at = b.created_at) b
    on
                a.github_id = b.github_id and

                a.commite_date = b.created_at;"""
    # table_name = 'metrics_day_timeline'
    insert_sql = f"insert into {table_name} values"
    logger.info(f"计算owner: {owner},repo: {repo}        metrics......")
    results = ck.execute_no_params(sql)
    bulk_data = []
    count = 0
    for result in results:
        count += 1
        data_dict = {}
        data_dict["ck_data_insert_at"] = int(time.time() * 1000)
        data_dict["owner"] = result[0]
        data_dict["repo"] = result[1]
        data_dict["created_at"] = datetime.combine(result[2], datetime.min.time())
        data_dict["github_id"] = result[3]
        # data_dict["github_login"] = result[4]
        # data_dict["git_author_email"] = result[5]
        data_dict["commit_times"] = result[4]
        data_dict["changed_lines"] = result[5]
        data_dict["diff_file_counts"] = result[6]
        data_dict["prs_counts"] = result[7]
        data_dict["pr_body_length_avg"] = result[8]
        data_dict["pr_body_length_sum"] = result[9]
        data_dict["issues_counts"] = result[10]
        data_dict["issue_body_length_avg"] = result[11]
        data_dict["issue_body_length_sum"] = result[12]
        data_dict["issues_comment_count"] = result[13]
        data_dict["issues_comment_body_length_avg"] = result[14]
        data_dict["issues_comment_body_length_sum"] = result[15]
        data_dict["pr_comment_count"] = result[16]
        data_dict["pr_comment_body_length_avg"] = result[17]
        data_dict["pr_comment_body_length_sum"] = result[18]
        data_dict["be_mentioned_times_in_issues"] = result[19]
        data_dict["be_mentioned_times_in_pr"] = result[20]
        data_dict["referred_other_issues_or_prs_in_issue"] = result[21]
        data_dict["referred_other_issues_or_prs_in_pr"] = result[22]
        data_dict["changed_label_times_in_issues"] = result[23]
        data_dict["changed_label_times_in_prs"] = result[24]
        data_dict["closed_issues_times"] = result[25]
        data_dict["closed_prs_times"] = result[26]

        bulk_data.append(data_dict)

        if len(bulk_data) == 20000:
            ck.execute(sql=insert_sql, params=bulk_data)
            logger.info(f"已经插入{count}条")
            bulk_data.clear()
    if bulk_data:
        ck.execute(sql=insert_sql, params=bulk_data)
        logger.info(f"已经插入{count}条")
    logger.info("wait 30 sec........")
    time.sleep(30)
    if not if_data_eq_github(count=count, ck=ck, table_name=table_name, owner=owner, repo=repo):
        raise Exception("Insert failed：Inconsistent data count")
    else:
        logger.info("Successfully inserted: datas are consistent")
    ck.close()



def if_data_eq_github(count, ck, table_name, owner, repo):
    sql = f"select count() from {table_name} where owner='{owner}' and repo='{repo}'"
    result = ck.execute_no_params(sql)
    logger.info(f'data count in ck {result[0][0]}')
    return count == result[0][0]



