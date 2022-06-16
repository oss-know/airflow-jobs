import numpy as np

from oss_know.libs.util.clickhouse_driver import CKServer
from oss_know.libs.util.log import logger


def statistics_metrics_by_repo(clickhouse_server_info, owner, repo):
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])
    results = ck.execute_no_params(f"""
    select
    if(a.owner != '',
          a.owner,
          b.owner)        as owner,
       if(a.repo != '',
          a.repo,
          b.repo)         as repo,

       if(a.github_id != 0,
          a.github_id,
          b.github_id)    as github_id,
    if(a.github_login != '',
          a.github_login,
          b.github_login)    as github_login,
                   git_author_email,

       commit_times,
       all_lines,
       file_counts,
       user_prs_counts,
       pr_body_length_avg,
--        pr_body_length_sum,
       user_issues_counts,
       issue_body_length_avg,
--        issue_body_length_sum,
       issues_comment_count,
       issues_comment_body_length_avg,
--        issues_comment_body_length_sum,
       pr_comment_count,
       pr_comment_body_length_avg,
--        pr_comment_body_length_sum,
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
            git_author_email,
            github_id,
            github_login,
            sum(commit_times) as commit_times,
            sum(all_lines)    as all_lines,
            sum(file_counts)  as file_counts
     from (select a.search_key__owner as owner,
                  a.search_key__repo  as repo,

                  b.author__id        as github_id,
                  b.author__login as github_login,
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

                                    COUNT(author_email) as commit_times,
                                    SUM(total__lines)   as all_lines
                             from gits
                             where search_key__owner = '{owner}'
                               and search_key__repo = '{repo}'
                               and author_email != ''
                             group by search_key__owner,
                                      search_key__repo,
                                      author_email
                             ) commits_all_lines
                             GLOBAL
                             full JOIN
                         (
                             --统计这个人更改过多少的不同的文件
                             select search_key__owner,
                                    search_key__repo,
                                    author_email,
                                    COUNT(*) file_counts
                             from (
                                      select DISTINCT `search_key__owner`,
                                                      `search_key__repo`,
                                                      `author_email`,
                                                      `files.file_name` as   file_name
                                      from `gits`
                                               array join `files.file_name`
                                      where search_key__owner = '{owner}'
                                        and search_key__repo = '{repo}'
                                        and author_email != '')
                             group by search_key__owner,
                                      search_key__repo,
                                      author_email) changed_files
                         ON
                                     commits_all_lines.search_key__owner = changed_files.search_key__owner
                                 AND
                                     commits_all_lines.search_key__repo = changed_files.search_key__repo
                                 AND
                                     commits_all_lines.author_email = changed_files.author_email
) a
                    GLOBAL
                    JOIN
                (
                    select DISTINCT search_key__owner,
                                    search_key__repo,
                                    commit__author__email,
                                    author__id,
                                    author__login
                    from github_commits gct
                    where author__id != 0
                      and search_key__owner = '{owner}'
                      and search_key__repo = '{repo}') b
                on
                            a.search_key__owner = b.search_key__owner
                        and a.search_key__repo = b.search_key__repo
                        and a.author_email = b.commit__author__email)
     group by owner, repo,  github_id,github_login,git_author_email
        ) a
        global
        full join
    -- 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19
        (select
             if(a.owner != '',
                   a.owner,
                   b.owner)        as owner,
                if(a.repo != '',
                   a.repo,
                   b.repo)         as repo,

                if(a.github_id != '',
                   toInt64(a.github_id),
                   b.github_id)    as github_id,
             if(a.github_login != '',
                   a.github_login,
                   b.github_login)    as github_login,

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
             (select
                  if(a.owner != '',
                        a.owner,
                        b.owner)        as owner,
                     if(a.repo != '',
                        a.repo,
                        b.repo)         as repo,

                     if(a.github_id != '',
                        a.github_id,
                        b.github_id)    as github_id,
                  if(a.github_login != '',
                        a.github_login,
                        b.github_login)    as github_login,

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
                  (select
                       if(a.owner != '',
                             a.owner,
                             b.owner)        as owner,
                          if(a.repo != '',
                             a.repo,
                             b.repo)         as repo,

                          if(a.github_id != '',
                             a.github_id,
                             b.github_id)    as github_id,
                       if(a.github_login != '',
                             a.github_login,
                             b.github_login)    as github_login,

                          be_mentioned_times_in_issues,
                          be_mentioned_times_in_pr,
                          referred_other_issues_or_prs_in_issue,
                          referred_other_issues_or_prs_in_pr
                   from (
                            --12,13
                            select
                                if(a.search_key__owner != '',
                                      a.search_key__owner,
                                      b.search_key__owner) as owner,
                                   if(a.search_key__repo != '',
                                      a.search_key__repo,
                                      b.search_key__repo)  as repo,

                                   if(a.id != '',
                                      a.id,
                                      b.id)                as github_id,
                                if(a.login != '',
                                      a.login,
                                      b.login)                as github_login,

                                   be_mentioned_times_in_issues,
                                   be_mentioned_times_in_pr
                            from (
                                     select search_key__owner,
                                            search_key__repo,

                                            id,
                                            login,
                                            COUNT() as be_mentioned_times_in_issues
                                     from (
                                              select search_key__owner,
                                                     search_key__repo,

                                                     JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'id')    as id,
                                              JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'login')    as login

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

                                              id,
                                              login
                                              ) a
                                     GLOBAL
                                     FULL JOIN
                                 (
                                     select search_key__owner,
                                            search_key__repo,

                                            id,
                                            login,
                                            COUNT() as be_mentioned_times_in_pr
                                     from (
                                              select search_key__owner,
                                                     search_key__repo,

                                                     JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'id')    as id,
                                                  JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'login')    as login
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

                                              id,
                                              login
                                              ) b
                                 ON
                                             a.id = b.id
                                             and a.login=b.login

                                         and a.search_key__owner = b.search_key__owner
                                         and a.search_key__repo = b.search_key__repo) a
                            GLOBAL
                            FULL JOIN
                        (
                            --14 15
                            select
                                if(a.search_key__owner != '',
                                      a.search_key__owner,
                                      b.search_key__owner) as owner,
                                   if(a.search_key__repo != '',
                                      a.search_key__repo,
                                      b.search_key__repo)  as repo,
                                   if(a.id != '',
                                      a.id,
                                      b.id)                as github_id,
if(a.login != '',
                                      a.login,
                                      b.login)                as github_login,
                                   referred_other_issues_or_prs_in_issue,
                                   referred_other_issues_or_prs_in_pr
                            from (
                                     select cross_referenced.search_key__owner,
                                            cross_referenced.search_key__repo,
                                            id,
login,
                                            COUNT(id) as referred_other_issues_or_prs_in_issue
                                     from (
                                              select search_key__owner,
                                                     search_key__repo,

                                                     JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'id')    as id,
                                                  JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'login')    as login
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
                                              id,
                                              login
                                              ) a
                                     GLOBAL
                                     FULL JOIN
                                 (
                                     select cross_referenced.search_key__owner,
                                            cross_referenced.search_key__repo,

                                            id,
login,
                                            COUNT(id) as referred_other_issues_or_prs_in_pr
                                     from (
                                              select search_key__owner,
                                                     search_key__repo,

                                                     JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'id')    as id,
                                                  JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'login')    as login

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
                                              id,
                                              login
                                              ) b
                                 ON
                                             a.id = b.id
                                        and a.login = b.login

                                         and a.search_key__owner = b.search_key__owner
                                         and a.search_key__repo = b.search_key__repo) b
                        ON
                                    a.github_id = b.github_id
                                and a.github_login=b.github_login

                                and a.owner = b.owner
                                and a.repo = b.repo) a
                      global
                      full join
                  --16 17 18 19
                      (select
                           if(a.owner != '',
                                 a.owner,
                                 b.owner)        as owner,
                              if(a.repo != '',
                                 a.repo,
                                 b.repo)         as repo,

                              if(a.github_id != '',
                                 a.github_id,
                                 b.github_id)    as github_id,
                           if(a.github_login != '',
                                 a.github_login,
                                 b.github_login)    as github_login,

                              changed_label_times_in_issues,
                              changed_label_times_in_prs,
                              closed_issues_times,
                              closed_prs_times
                       from (
                                --12 13
                                select
                                    if(a.search_key__owner != '',
                                          a.search_key__owner,
                                          b.search_key__owner) as owner,
                                       if(a.search_key__repo != '',
                                          a.search_key__repo,
                                          b.search_key__repo)  as repo,

                                       if(a.id != '',
                                          a.id,
                                          b.id)                as github_id,
                                    if(a.login != '',
                                          a.login,
                                          b.login)                as github_login,

                                       changed_label_times_in_issues,
                                       changed_label_times_in_prs
                                from (
                                         select github_issues_timeline.search_key__owner,
                                                github_issues_timeline.search_key__repo,

                                                JSONExtractString(JSONExtractString(timeline_raw,
                                                                                    'actor'),
                                                                  'id')    as id,
                                             JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'login')    as login,

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
                                                  id,
                                                  login
                                                  ) a
                                         GLOBAL
                                         FULL JOIN
                                     (
                                         select github_issues_timeline.search_key__owner,
                                                github_issues_timeline.search_key__repo,

                                                JSONExtractString(JSONExtractString(timeline_raw,
                                                                                    'actor'),
                                                                  'id')    as id,
                                             JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'login')    as login,

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
                                                  id,login) b
                                     ON
                                                 a.id = b.id
                                                     and a.login=b.login
                                             and a.search_key__owner = b.search_key__owner
                                             and a.search_key__repo = b.search_key__repo
) a
                                GLOBAL
                                FULL JOIN
                            (
                                --14 15
                                select
                                    if(a.search_key__owner != '',
                                          a.search_key__owner,
                                          b.search_key__owner) as owner,
                                       if(a.search_key__repo != '',
                                          a.search_key__repo,
                                          b.search_key__repo)  as repo,
                                       if(a.actor_id != '',
                                          a.actor_id,
                                          b.actor_id)          as github_id,
                                    if(a.login != '',
                                          a.login,
                                          b.login)          as github_login,

                                       closed_issues_times,
                                       closed_prs_times
                                from (
                                         select search_key__owner,
                                                search_key__repo,

                                                JSONExtractString(JSONExtractString(timeline_raw,
                                                                                    'actor'),
                                                                  'id')    as actor_id,
                                             JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'login')    as login,

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
                                                  actor_id,login) a
                                         GLOBAL
                                         FULL JOIN
                                     (
                                         select search_key__owner,
                                                search_key__repo,

                                                JSONExtractString(JSONExtractString(timeline_raw,
                                                                                    'actor'),
                                                                  'id')    as actor_id,
                                             JSONExtractString(JSONExtractString(timeline_raw,
                                                                                         'actor'),
                                                                       'login')    as login,

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
                                                  actor_id,login) b
                                     ON
                                                 a.actor_id = b.actor_id
                                             and a.search_key__owner = b.search_key__owner
                                             and a.search_key__repo = b.search_key__repo) b
                            ON
                                        a.github_id = b.github_id
                                            and a.github_login=b.github_login
                                    and a.owner = b.owner
                                    and a.repo = b.repo) b
                  on
                              a.github_id = b.github_id and a.github_login=b.github_login) a
                 global
                 full join
             -- 4 5 6 7 8 9 10 11
                 (select
                      if(a.owner != '',
                            a.owner,
                            b.owner)        as owner,
                         if(a.repo != '',
                            a.repo,
                            b.repo)         as repo,
                         if(a.github_id != 0,
                            a.github_id,
                            b.github_id)    as github_id,
                      if(a.github_login != '',
                                 a.github_login,
                                 b.github_login)          as github_login,
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
                      (select
                           if(a.search_key__owner != '',
                                 a.search_key__owner,
                                 b.search_key__owner) as owner,
                              if(a.search_key__repo != '',
                                 a.search_key__repo,
                                 b.search_key__repo)  as repo,
                              if(a.user__id != 0,
                                 a.user__id,
                                 b.user__id)          as github_id,
                           if(a.user__login != '',
                                 a.user__login,
                                 b.user__login)          as github_login,
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
                                       `user__id`,
                                       user__login,
                                       COUNT(user__id) as    `user_prs_counts`,
                                       sum(lengthUTF8(body)) body_length_sum,
                                       avg(lengthUTF8(body)) `body_length_avg`
                                from github_pull_requests
                                where search_key__owner = '{owner}'
                                  and search_key__repo = '{repo}'
                                group by `search_key__owner`,
                                         `search_key__repo`,
                                         `user__id`,user__login) a
                                GLOBAL
                                FULL JOIN
                            (
                                -- 6 7
                                select `search_key__owner`,
                                       `search_key__repo`,
                                       `user__id`,
                                       user__login,
                                       COUNT(user__id) as    `user_issues_counts`,
                                       sum(lengthUTF8(body)) body_length_sum,
                                       avg(lengthUTF8(body)) `body_length_avg`
                                from github_issues
                                where pull_request__url == ''
                                  and search_key__owner = '{owner}'
                                  and search_key__repo = '{repo}'
                                group by `search_key__owner`,
                                         `search_key__repo`,
                                         `user__id`,user__login) b
                            ON
                                        a.user__id = b.user__id
                                    and a.user__login=b.user__login
                                    and a.search_key__owner = b.search_key__owner
                                    and a.search_key__repo = b.search_key__repo) a
                          global
                          full join
                      -- 8 9 10 11
                          (select
                               if(a.search_key__owner != '',
                                     a.search_key__owner,
                                     b.search_key__owner) as owner,
                                  if(a.search_key__repo != '',
                                     a.search_key__repo,
                                     b.search_key__repo)  as repo,
                                  if(a.user__id != 0,
                                     a.user__id,
                                     b.user__id)          as github_id,
                               if(a.user__login != '',
                                 a.user__login,
                                 b.user__login)          as github_login,

                                  a.issues_comment_count,
                                  a.issues_comment_body_length_avg,
                                  a.body_length_sum       as issues_comment_body_length_sum,
                                  b.pr_comment_count,
                                  b.pr_comment_body_length_avg,
                                  b.body_length_sum       as pr_comment_body_length_sum
                           from (
                                    select search_key__owner,
                                           search_key__repo,
                                           user__id,
                                           user__login,
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
                                             user__id,user__login) a
                                    GLOBAL
                                    FULL JOIN
                                (
                                    select search_key__owner,
                                           search_key__repo,
                                           user__id,
                                           user__login,
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
                                             user__id,user__login) b
                                ON
                                            a.user__id = b.user__id
                                            and a.user__login=b.user__login
                                        and a.search_key__owner = b.search_key__owner
                                        and a.search_key__repo = b.search_key__repo) b
                      on
                                  a.github_id = b.github_id and a.github_login=b.github_login) b
             on
                         toInt64(a.github_id) = b.github_id
            and a.github_login=b.github_login
) b
    on
                a.github_id = b.github_id and a.github_login=b.github_login""")
    all_data = []
    ck_sql = "INSERT INTO metrics VALUES"
    for result in results:
        data = {}
        data['owner'] = result[0]
        data['repo'] = result[1]
        data['github_id'] = int(result[2])
        data['github_login'] = result[3]
        data['git_author_email'] = result[4]
        data['commit_times'] = result[5]
        data['changed_lines'] = result[6]
        data['diff_file_counts'] = result[7]
        data['prs_counts'] = result[8]
        data['pr_body_length_avg'] = result[9]
        data['issues_counts'] = result[10]
        data['issue_body_length_avg'] = result[11]
        data['issues_comment_count'] = result[12]
        data['issues_comment_body_length_avg'] = result[13]
        data['pr_comment_count'] = result[14]
        data['pr_comment_body_length_avg'] = result[15]
        data['be_mentioned_times_in_issues'] = result[16]
        data['be_mentioned_times_in_pr'] = result[17]
        data['referred_other_issues_or_prs_in_issue'] = result[18]
        data['referred_other_issues_or_prs_in_pr'] = result[19]
        data['changed_label_times_in_issues'] = result[20]
        data['changed_label_times_in_prs'] = result[21]
        data['closed_issues_times'] = result[22]
        data['closed_prs_times'] = result[23]
        all_data.append(data)
        if len(all_data) >= 10000:
            response = ck.execute(ck_sql, all_data)
            logger.info(f"INSERT INTO metrics {response}")
            all_data.clear()
    if all_data:
        response = ck.execute(ck_sql, all_data)
        logger.info(f"INSERT INTO metrics {response}")

    return "end::statistics_metrics"


def statistics_metrics(clickhouse_server_info):
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])
    results = ck.execute_no_params("""
       select * from ( --1--15
        select 
    	if(a.owner != '',
    	a.owner,
    	b.owner) as owner,
    	if(a.repo != '',
    	a.repo,
    	b.repo) as repo,
    	if(a.github_id != 0,
    	toString(a.github_id),
    	b.github_id) as github_id,
    	if(a.github_id != 0,
    	a.github_login,
    	b.github_login) as github_login,
    	if(a.git_author_email != '',
    	a.git_author_email,
    	'') as git_author_email,
    	commit_times,
    	all_lines,
    	file_counts,
    	user_prs_counts,
    	pr_body_length_avg,
    	user_issues_counts,
    	issue_body_length_avg,
    	issues_comment_count,
    	issues_comment_body_length_avg,
    	pr_comment_count,
    	pr_comment_body_length_avg,
    	be_mentioned_times_in_issues,
    	be_mentioned_times_in_pr,
    	referred_other_issues_or_prs_in_issue,
    	referred_other_issues_or_prs_in_pr,
    	changed_label_times_in_issues,
    	changed_label_times_in_prs,
    	closed_issues_times,
    	closed_prs_times
    from
    	(
    	--1--3
    	--提交了多少次commit，更改了多少行,更改过多少文件 (将对不上账号的剔除掉)
    	select
    		a.search_key__owner as owner,
    		a.search_key__repo as repo,
    		b.author__id as github_id,
    		b.author__login as github_login,
    		author_email as git_author_email,
    		commit_times,
    		all_lines,
    		file_counts
    	from
    		(
    		select
    			commits_all_lines.*,
    			changed_files.file_counts
    		from
    			(
    			select
    				search_key__owner,
    				search_key__repo,
    				author_email,
    				COUNT(author_email) as commit_times,
    				SUM(total__lines) as all_lines
    			from
    				gits
    			group by
    				search_key__owner ,
    				search_key__repo ,
    				author_email
    ) commits_all_lines
    GLOBAL
    		FULL JOIN 
    (
    			select
    				search_key__owner,
    				search_key__repo,
    				author_email,
    				COUNT(*) file_counts
    			from
    				(
    				select
    					DISTINCT `search_key__owner`,
    					`search_key__repo`,
    					`author_email`,
    					`files.file_name` as file_name
    				from
    					`gits`
                   array
    				join `files.file_name`)
    			group by
    				search_key__owner ,
    				search_key__repo ,
    				author_email) changed_files
     ON
    			commits_all_lines.search_key__owner = changed_files.search_key__owner
    			AND 
     commits_all_lines.search_key__repo = changed_files.search_key__repo
    			AND 
     commits_all_lines.author_email = changed_files.author_email ) a
    GLOBAL
    	RIGHT JOIN 
    (
    		select
    			DISTINCT search_key__owner ,
    			search_key__repo ,
    			commit__author__email ,
    			author__id,
    			author__login
    		from
    			github_commits gct
    		where
    			author__id != 0 ) b
    on
    		a.search_key__owner = b.search_key__owner
    		and a.search_key__repo = b.search_key__repo
    		and a.author_email = b.commit__author__email) a 
    GLOBAL
    FULL JOIN 
    (
    	--4 5 6 7 8 9 10 11 12 13 14 15
    	select
    		if(a.owner != '',
    		a.owner,
    		b.owner) as owner,
    		if(a.repo != '',
    		a.repo,
    		b.repo) as repo,
    		if(a.github_id != 0,
    		toString(a.github_id),
    		b.github_id) as github_id,
    		if(a.github_id != 0,
    		a.github_login,
    		b.github_login) as github_login,
    		user_prs_counts,
    		pr_body_length_avg,
    		user_issues_counts,
    		issue_body_length_avg,
    		issues_comment_count,
    		issues_comment_body_length_avg,
    		pr_comment_count,
    		pr_comment_body_length_avg,
    		be_mentioned_times_in_issues,
    		be_mentioned_times_in_pr,
    		referred_other_issues_or_prs_in_issue,
    		referred_other_issues_or_prs_in_pr,
    		changed_label_times_in_issues,
    		changed_label_times_in_prs,
    		closed_issues_times,
    		closed_prs_times
    	from
    		(
    		--指标4，5，6，7
    		select
    			if(c.owner != '',
    			c.owner,
    			d.owner) as owner,
    			if(c.repo != '',
    			c.repo,
    			d.repo) as repo,
    			if(c.github_id != 0,
    			c.github_id,
    			d.github_id) as github_id,
    			if(c.github_id != 0,
    			c.github_login,
    			d.github_login) as github_login,
    			user_prs_counts,
    			pr_body_length_avg,
    			user_issues_counts,
    			issue_body_length_avg,
    			issues_comment_count,
    			issues_comment_body_length_avg,
    			pr_comment_count,
    			pr_comment_body_length_avg
    		from
    			(
    			select
    				if(a.search_key__owner != '',
    				a.search_key__owner,
    				b.search_key__owner) as owner,
    				if(a.search_key__repo != '',
    				a.search_key__repo,
    				b.search_key__repo) as repo,
    				if(a.user__id != 0,
    				a.user__id,
    				b.user__id) as github_id,
    				if(a.user__id != 0,
    				a.user__login,
    				b.user__login) as github_login,
    				a.user_prs_counts,
    				a.body_length_avg as pr_body_length_avg,
    				b.user_issues_counts,
    				b.body_length_avg as issue_body_length_avg
    			from
    				(
    				select
    					`search_key__owner`,
    					`search_key__repo`,
    					`user__id`,
    					user__login,
    					COUNT(user__id) as `user_prs_counts`,
    					avg(lengthUTF8(body)) `body_length_avg`
    				from
    					github_pull_requests
    				group by
    					`search_key__owner`,
    					`search_key__repo`,
    					`user__id`,
    					user__login) a 
    GLOBAL
    			FULL JOIN 
    (
    				select
    					`search_key__owner`,
    					`search_key__repo`,
    					`user__id`,
    					user__login,
    					COUNT(user__id) as `user_issues_counts`,
    					avg(lengthUTF8(body)) `body_length_avg`
    				from
    					github_issues
    				where
    					pull_request__url == ''
    				group by
    					`search_key__owner`,
    					`search_key__repo`,
    					`user__id`,
    					user__login) b
    ON
    				a.user__id = b.user__id
    				and a.search_key__owner = b.search_key__owner
    				and a.search_key__repo = b.search_key__repo) c
    GLOBAL
    		FULL JOIN 
    (
    			select
    				if(a.search_key__owner != '',
    				a.search_key__owner,
    				b.search_key__owner) as owner,
    				if(a.search_key__repo != '',
    				a.search_key__repo,
    				b.search_key__repo) as repo,
    				if(a.user__id != 0,
    				a.user__id,
    				b.user__id) as github_id,
    				if(a.user__id != 0,
    				a.user__login,
    				b.user__login) as github_login,
    				a.issues_comment_count,
    				a.issues_comment_body_length_avg,
    				b.pr_comment_count,
    				b.pr_comment_body_length_avg
    			from
    				(
    				select
    					search_key__owner,
    					search_key__repo,
    					user__id,
    					user__login,
    					COUNT() as issues_comment_count,
    					avg(lengthUTF8(body)) issues_comment_body_length_avg
    				from
    					(
    					select
    						github_issues_comments.*
    					from
    						github_issues_comments global
    					join (
    						select
    							DISTINCT `number`,
    							search_key__owner,
    							search_key__repo
    						from
    							github_issues gict
    						WHERE
    							pull_request__url = '') as pr_number
    on
    						github_issues_comments.search_key__number = pr_number.number
    						and github_issues_comments.search_key__owner = pr_number.search_key__owner
    						and github_issues_comments.search_key__repo = pr_number.search_key__repo )
    				group by
    					search_key__owner,
    					search_key__repo,
    					user__id,
    					user__login) a 
    GLOBAL
    			FULL JOIN 
    (
    				select
    					search_key__owner,
    					search_key__repo,
    					user__id,
    					user__login,
    					COUNT() as pr_comment_count,
    					avg(lengthUTF8(body)) pr_comment_body_length_avg
    				from
    					(
    					select
    						github_issues_comments.*
    					from
    						github_issues_comments global
    					join (
    						select
    							DISTINCT `number`,
    							search_key__owner,
    							search_key__repo
    						from
    							github_issues gict
    						WHERE
    							pull_request__url != '') as pr_number
    on
    						github_issues_comments.search_key__number = pr_number.number
    						and github_issues_comments.search_key__owner = pr_number.search_key__owner
    						and github_issues_comments.search_key__repo = pr_number.search_key__repo )
    				group by
    					search_key__owner,
    					search_key__repo,
    					user__id,
    					user__login) b
    ON
    				a.user__id = b.user__id
    				and a.search_key__owner = b.search_key__owner
    				and a.search_key__repo = b.search_key__repo) d
    ON
    			c.github_id = d.github_id
    			and c.owner = d.owner
    			and c.repo = d.repo) a 
    GLOBAL
    	FULL JOIN 
    (
    		-- 8 9 10 11 12 13 14 15
    		select
    			if(a.owner != '',
    			a.owner,
    			b.owner) as owner,
    			if(a.repo != '',
    			a.repo,
    			b.repo) as repo,
    			if(a.github_id != '',
    			a.github_id,
    			b.github_id) as github_id,
    			if(a.github_id != '',
    			a.github_login,
    			b.github_login) as github_login,
    			be_mentioned_times_in_issues,
    			be_mentioned_times_in_pr,
    			referred_other_issues_or_prs_in_issue,
    			referred_other_issues_or_prs_in_pr,
    			changed_label_times_in_issues,
    			changed_label_times_in_prs,
    			closed_issues_times,
    			closed_prs_times
    		from
    			(
    			--8 9 10 11
    			select
    				if(a.owner != '',
    				a.owner,
    				b.owner) as owner,
    				if(a.repo != '',
    				a.repo,
    				b.repo) as repo,
    				if(a.github_id != '',
    				a.github_id,
    				b.github_id) as github_id,
    				if(a.github_id != '',
    				a.github_login,
    				b.github_login) as github_login,
    				be_mentioned_times_in_issues,
    				be_mentioned_times_in_pr,
    				referred_other_issues_or_prs_in_issue,
    				referred_other_issues_or_prs_in_pr
    			from
    				(
    				--8,9
    				select
    					if(a.search_key__owner != '',
    					a.search_key__owner,
    					b.search_key__owner) as owner,
    					if(a.search_key__repo != '',
    					a.search_key__repo,
    					b.search_key__repo) as repo,
    					if(a.id != '',
    					a.id,
    					b.id) as github_id,
    					if(a.id != '',
    					a.login,
    					b.login) as github_login,
    					be_mentioned_times_in_issues,
    					be_mentioned_times_in_pr
    				from
    					(
    					select
    						search_key__owner,
    						search_key__repo,
    						id,
    						login,
    						COUNT() as be_mentioned_times_in_issues
    					from
    						(
    						select
    							search_key__owner,
    							search_key__repo,
    							JSONExtractString(JSONExtractString(timeline_raw,
    							'actor'),
    							'id') as id,
    							JSONExtractString(JSONExtractString(timeline_raw,
    							'actor'),
    							'login') as login
    						from
    							(
    							select
    								github_issues_timeline.*
    							from
    								github_issues_timeline global
    							join (
    								select
    									DISTINCT `number`,
    									search_key__owner ,
    									search_key__repo
    								from
    									github_issues gict
    								WHERE
    									pull_request__url = '') as pr_number
    on
    								github_issues_timeline.search_key__number = pr_number.number
    								and github_issues_timeline.search_key__owner = pr_number.search_key__owner
    								and github_issues_timeline.search_key__repo = pr_number.search_key__repo)
    						WHERE
    							JSONExtractString(timeline_raw,
    							'event')= 'mentioned'
    								and JSONHas(JSONExtractString(timeline_raw,
    								'actor'),
    								'id')= 1
    )
    					group by
    						search_key__owner,
    						search_key__repo,
    						id,
    						login) a 
    GLOBAL
    				FULL JOIN 
    (
    					select
    						search_key__owner,
    						search_key__repo,
    						id,
    						login,
    						COUNT() as be_mentioned_times_in_pr
    					from
    						(
    						select
    							search_key__owner,
    							search_key__repo,
    							JSONExtractString(JSONExtractString(timeline_raw,
    							'actor'),
    							'id') as id,
    							JSONExtractString(JSONExtractString(timeline_raw,
    							'actor'),
    							'login') as login
    						from
    							(
    							select
    								github_issues_timeline.*
    							from
    								github_issues_timeline global
    							join (
    								select
    									DISTINCT `number`,
    									search_key__owner ,
    									search_key__repo
    								from
    									github_issues gict
    								WHERE
    									pull_request__url != '') as pr_number
    on
    								github_issues_timeline.search_key__number = pr_number.number
    								and github_issues_timeline.search_key__owner = pr_number.search_key__owner
    								and github_issues_timeline.search_key__repo = pr_number.search_key__repo)
    						WHERE
    							JSONExtractString(timeline_raw,
    							'event')= 'mentioned'
    								and JSONHas(JSONExtractString(timeline_raw,
    								'actor'),
    								'id')= 1
    )
    					group by
    						search_key__owner,
    						search_key__repo,
    						id,
    						login) b
    ON
    					a.id = b.id
    					and a.search_key__owner = b.search_key__owner
    					and a.search_key__repo = b.search_key__repo) a 
    GLOBAL
    			FULL JOIN 
    (
    				--10 11
    				select
    					if(a.search_key__owner != '',
    					a.search_key__owner,
    					b.search_key__owner) as owner,
    					if(a.search_key__repo != '',
    					a.search_key__repo,
    					b.search_key__repo) as repo,
    					if(a.id != '',
    					a.id,
    					b.id) as github_id,
    					if(a.id != '',
    					a.login,
    					b.login) as github_login,
    					referred_other_issues_or_prs_in_issue,
    					referred_other_issues_or_prs_in_pr
    				from
    					(
    					select
    						cross_referenced.search_key__owner,
    						cross_referenced.search_key__repo,
    						id ,
    						login,
    						COUNT(id) as referred_other_issues_or_prs_in_issue
    					from
    						(
    						select
    							search_key__owner ,
    							search_key__repo ,
    							JSONExtractString(JSONExtractString(timeline_raw,
    							'actor'),
    							'id') as id,
    							JSONExtractString(JSONExtractString(timeline_raw,
    							'actor'),
    							'login') as login,
    							JSONExtractString(JSONExtractString(JSONExtractString(timeline_raw,
    							'source'),
    							'issue'),
    							'number') as number
    						from
    							github_issues_timeline
    						WHERE
    							JSONExtractString(timeline_raw,
    							'event') = 'cross-referenced'
    								AND JSONHas(JSONExtractString(timeline_raw,
    								'actor'),
    								'id')= 1) cross_referenced
    GLOBAL
    					JOIN 
    (
    						select
    							DISTINCT `number`,
    							search_key__owner ,
    							search_key__repo
    						from
    							github_issues gict
    						WHERE
    							pull_request__url = '') issues
    ON
    						cross_referenced.search_key__owner = issues.search_key__owner
    						and 
    	cross_referenced.search_key__repo = issues.search_key__repo
    						and
    	cross_referenced.number = toString(issues.number)
    					GROUP BY
    						cross_referenced.search_key__owner ,
    						cross_referenced.search_key__repo,
    						id,
    						login) a 
    GLOBAL
    				FULL JOIN 
    (
    					select
    						cross_referenced.search_key__owner,
    						cross_referenced.search_key__repo,
    						id ,
    						login,
    						COUNT(id) as referred_other_issues_or_prs_in_pr
    					from
    						(
    						select
    							search_key__owner ,
    							search_key__repo ,
    							JSONExtractString(JSONExtractString(timeline_raw,
    							'actor'),
    							'id') as id,
    							JSONExtractString(JSONExtractString(timeline_raw,
    							'actor'),
    							'login') as login,
    							JSONExtractString(JSONExtractString(JSONExtractString(timeline_raw,
    							'source'),
    							'issue'),
    							'number') as number
    						from
    							github_issues_timeline
    						WHERE
    							JSONExtractString(timeline_raw,
    							'event') = 'cross-referenced'
    								AND JSONHas(JSONExtractString(timeline_raw,
    								'actor'),
    								'id')= 1) cross_referenced
    GLOBAL
    					JOIN 
    (
    						select
    							DISTINCT `number`,
    							search_key__owner ,
    							search_key__repo
    						from
    							github_issues gict
    						WHERE
    							pull_request__url != '') issues
    ON
    						cross_referenced.search_key__owner = issues.search_key__owner
    						and 
    	cross_referenced.search_key__repo = issues.search_key__repo
    						and
    	cross_referenced.number = toString(issues.number)
    					GROUP BY
    						cross_referenced.search_key__owner ,
    						cross_referenced.search_key__repo,
    						id,
    						login) b
    ON
    					a.id = b.id
    					and a.search_key__owner = b.search_key__owner
    					and a.search_key__repo = b.search_key__repo) b
    ON
    				a.github_id = b.github_id
    				and a.owner = b.owner
    				and a.repo = b.repo) a 
    GLOBAL
    		FULL JOIN 
    (
    			-- 12 13 14 15
    			select
    				if(a.owner != '',
    				a.owner,
    				b.owner) as owner,
    				if(a.repo != '',
    				a.repo,
    				b.repo) as repo,
    				if(a.github_id != '',
    				a.github_id,
    				b.github_id) as github_id,
    				if(a.github_id != '',
    				a.github_login,
    				b.github_login) as github_login,
    				changed_label_times_in_issues,
    				changed_label_times_in_prs,
    				closed_issues_times,
    				closed_prs_times
    			from
    				(
    				--12 13
    				select
    					if(a.search_key__owner != '',
    					a.search_key__owner,
    					b.search_key__owner) as owner,
    					if(a.search_key__repo != '',
    					a.search_key__repo,
    					b.search_key__repo) as repo,
    					if(a.id != '',
    					a.id,
    					b.id) as github_id,
    					if(a.id != '',
    					a.login,
    					b.login) as github_login,
    					changed_label_times_in_issues,
    					changed_label_times_in_prs
    				from
    					(
    					select
    						github_issues_timeline.search_key__owner ,
    						github_issues_timeline.search_key__repo ,
    						JSONExtractString(JSONExtractString(timeline_raw,
    						'actor'),
    						'id') as id,
    						JSONExtractString(JSONExtractString(timeline_raw,
    						'actor'),
    						'login') as login,
    						count(id) as changed_label_times_in_issues
    					from
    						github_issues_timeline  
    global
    					join 
    	(
    						select
    							DISTINCT `number`,
    							search_key__owner ,
    							search_key__repo
    						from
    							github_issues gict
    						WHERE
    							pull_request__url = '') as pr_number
    on
    						github_issues_timeline.search_key__number = pr_number.number
    						and
    	github_issues_timeline.search_key__owner = pr_number.search_key__owner
    						and
    	github_issues_timeline.search_key__repo = pr_number.search_key__repo
    					WHERE
    						(JSONExtractString(timeline_raw,
    						'event')= 'unlabeled'
    							OR 
    	JSONExtractString(timeline_raw,
    							'event')= 'labeled')
    							AND 
    	JSONHas(JSONExtractString(timeline_raw,
    							'actor'),
    							'id')= 1
    						GROUP BY
    							github_issues_timeline.search_key__owner,
    							github_issues_timeline.search_key__repo,
    							id,
    							login) a 
    GLOBAL
    				FULL JOIN 
    (
    					select
    						github_issues_timeline.search_key__owner ,
    						github_issues_timeline.search_key__repo ,
    						JSONExtractString(JSONExtractString(timeline_raw,
    						'actor'),
    						'id') as id,
    						JSONExtractString(JSONExtractString(timeline_raw,
    						'actor'),
    						'login') as login,
    						count(id) as changed_label_times_in_prs
    					from
    						github_issues_timeline  
    global
    					join 
    	(
    						select
    							DISTINCT `number`,
    							search_key__owner ,
    							search_key__repo
    						from
    							github_issues gict
    						WHERE
    							pull_request__url != '') as pr_number
    on
    						github_issues_timeline.search_key__number = pr_number.number
    						and
    	github_issues_timeline.search_key__owner = pr_number.search_key__owner
    						and
    	github_issues_timeline.search_key__repo = pr_number.search_key__repo
    					WHERE
    						(JSONExtractString(timeline_raw,
    						'event')= 'unlabeled'
    							OR 
    	JSONExtractString(timeline_raw,
    							'event')= 'labeled')
    							AND 
    	JSONHas(JSONExtractString(timeline_raw,
    							'actor'),
    							'id')= 1
    						GROUP BY
    							github_issues_timeline.search_key__owner,
    							github_issues_timeline.search_key__repo,
    							id,
    							login) b
    ON
    					a.id = b.id
    					and a.search_key__owner = b.search_key__owner
    					and a.search_key__repo = b.search_key__repo) a 
    GLOBAL
    			FULL JOIN 
    (
    				--14 15
    				select
    					if(a.search_key__owner != '',
    					a.search_key__owner,
    					b.search_key__owner) as owner,
    					if(a.search_key__repo != '',
    					a.search_key__repo,
    					b.search_key__repo) as repo,
    					if(a.actor_id != '',
    					a.actor_id,
    					b.actor_id) as github_id,
    					if(a.actor_id != '',
    					a.actor_login,
    					b.actor_login) as github_login,
    					closed_issues_times,
    					closed_prs_times
    				from
    					(
    					select
    						search_key__owner,
    						search_key__repo,
    						JSONExtractString(JSONExtractString(timeline_raw,
    						'actor'),
    						'id') as actor_id,
    						JSONExtractString(JSONExtractString(timeline_raw,
    						'actor'),
    						'login') as actor_login,
    						COUNT() closed_issues_times
    					from
    						(
    						select
    							github_issues_timeline.*
    						from
    							github_issues_timeline global
    						join (
    							select
    								DISTINCT `number`,
    								search_key__owner ,
    								search_key__repo
    							from
    								github_issues gict
    							WHERE
    								pull_request__url = '') as pr_number
    on
    							github_issues_timeline.search_key__number = pr_number.number
    							and github_issues_timeline.search_key__owner = pr_number.search_key__owner
    							and github_issues_timeline.search_key__repo = pr_number.search_key__repo)
    					WHERE
    						JSONExtractString(timeline_raw,
    						'event')= 'closed'
    							AND JSONHas(JSONExtractString(timeline_raw,
    							'actor'),
    							'id')= 1
    						group by
    							search_key__owner,
    							search_key__repo,
    							actor_id,
    							actor_login) a 
    GLOBAL
    				FULL JOIN 
    (
    					select
    						search_key__owner,
    						search_key__repo,
    						JSONExtractString(JSONExtractString(timeline_raw,
    						'actor'),
    						'id') as actor_id,
    						JSONExtractString(JSONExtractString(timeline_raw,
    						'actor'),
    						'login') as actor_login,
    						COUNT() closed_prs_times
    					from
    						(
    						select
    							github_issues_timeline.*
    						from
    							github_issues_timeline global
    						join (
    							select
    								DISTINCT `number`,
    								search_key__owner ,
    								search_key__repo
    							from
    								github_issues gict
    							WHERE
    								pull_request__url != '') as pr_number
    on
    							github_issues_timeline.search_key__number = pr_number.number
    							and github_issues_timeline.search_key__owner = pr_number.search_key__owner
    							and github_issues_timeline.search_key__repo = pr_number.search_key__repo)
    					WHERE
    						JSONExtractString(timeline_raw,
    						'event')= 'closed'
    							AND JSONHas(JSONExtractString(timeline_raw,
    							'actor'),
    							'id')= 1
    						group by
    							search_key__owner,
    							search_key__repo,
    							actor_id,
    							actor_login) b
    ON
    					a.actor_id = b.actor_id
    					and a.search_key__owner = b.search_key__owner
    					and a.search_key__repo = b.search_key__repo) b
    ON
    				a.github_id = b.github_id
    				and a.owner = b.owner
    				and a.repo = b.repo) b
    ON
    			a.github_id = b.github_id
    			and a.owner = b.owner
    			and a.repo = b.repo
    ) b
    ON
    		toString(a.github_id)= b.github_id
    			and a.owner = b.owner
    			and a.repo = b.repo) b
    ON
    	toString(a.github_id)= b.github_id
    	and a.owner = b.owner
    	and a.repo = b.repo) order by owner""")
    all_data = []
    ck_sql = "INSERT INTO metrics VALUES"
    for result in results:
        data = {}
        data['owner'] = result[0]
        data['repo'] = result[1]
        data['github_id'] = int(result[2])
        data['github_login'] = result[3]
        data['git_author_email'] = result[4]
        data['commit_times'] = result[5]
        data['changed_lines'] = result[6]
        data['diff_file_counts'] = result[7]
        data['prs_counts'] = result[8]
        data['pr_body_length_avg'] = result[9]
        data['issues_counts'] = result[10]
        data['issue_body_length_avg'] = result[11]
        data['issues_comment_count'] = result[12]
        data['issues_comment_body_length_avg'] = result[13]
        data['pr_comment_count'] = result[14]
        data['pr_comment_body_length_avg'] = result[15]
        data['be_mentioned_times_in_issues'] = result[16]
        data['be_mentioned_times_in_pr'] = result[17]
        data['referred_other_issues_or_prs_in_issue'] = result[18]
        data['referred_other_issues_or_prs_in_pr'] = result[19]
        data['changed_label_times_in_issues'] = result[20]
        data['changed_label_times_in_prs'] = result[21]
        data['closed_issues_times'] = result[22]
        data['closed_prs_times'] = result[23]
        all_data.append(data)
        if len(all_data) > 10000:
            response = ck.execute(ck_sql, all_data)
            logger.info(f"INSERT INTO metrics {response}")
            all_data.clear()
    if all_data:
        response = ck.execute(ck_sql, all_data)
        logger.info(f"INSERT INTO metrics {response}")

    return "end::statistics_metrics"


get_metrics_sql = '''
select owner, repo, github_id, github_login, sum(commit_times), sum(changed_lines), sum(diff_file_counts),sum(prs_counts) / count(github_login), sum(pr_body_length_avg) / count(github_login), sum(issues_counts) / count(github_login), 
sum(issue_body_length_avg) / count(github_login), sum(issues_comment_count) / count(github_login), sum(issues_comment_body_length_avg) / count(github_login), sum(pr_comment_count) / count(github_login), 
sum(pr_comment_body_length_avg) / count(github_login), sum(be_mentioned_times_in_issues) / count(github_login), sum(be_mentioned_times_in_pr) / count(github_login), 
sum(referred_other_issues_or_prs_in_issue) / count(github_login), sum(referred_other_issues_or_prs_in_pr) / count(github_login), sum(changed_label_times_in_issues) / count(github_login), 
sum(changed_label_times_in_prs) / count(github_login), sum(closed_issues_times) / count(github_login), sum(closed_prs_times) / count(github_login) FROM  metrics 
group by (owner, repo, github_id, github_login) order by owner
'''

activity_factors = '''
0.00 1.03 0.02 -0.07 -0.04 0.05
0.00 1.03 0.02 -0.07 -0.04 0.05
0.00 0.26 -0.07 0.35 0.45 -0.17
0.06 -0.04 -0.11 0.45 0.70 0.05
0.05 -0.01 -0.03 -0.01 0.13 0.01
0.30 0.04 0.18 0.11 0.11 0.35
-0.01 0.00 -0.03 0.00 0.02 0.16
0.52 -0.01 0.30 0.24 -0.13 0.16
-0.02 0.01 0.00 -0.01 0.00 0.1
0.72 -0.01 0.09 0.03 0.25 -0.12
0.00 -0.01 0.04 -0.02 0.09 0.03
0.78 0.01 0.1 0.01 -0.02 0.14
1.02 0.01 -0.16 -0.2 0.2 -0.22
-0.12 0.02 1.06 -0.07 0.02 -0.15
0.06 0.00 0.59 -0.05 0.21 0.00
0.06 0.01 0.72 0.24 -0.23 -0.07
0.58 0.00 0.13 -0.03 0.15 -0.07
0.28 -0.03 0.08 0.65 -0.24 0.04
-0.30 -0.08 0.07 0.95 0.33 -0.06
'''
insert_factor_sql = '''
INSERT into activities values
'''


def statistics_activities(clickhouse_server_info, owner='', repo=''):
    read_ck_server = CKServer(host=clickhouse_server_info["HOST"],
                              port=clickhouse_server_info["PORT"],
                              user=clickhouse_server_info["USER"],
                              password=clickhouse_server_info["PASSWD"],
                              database=clickhouse_server_info["DATABASE"],
                              settings={'use_numpy': True})
    if owner == '' and repo == '':
        metrics = read_ck_server.execute_no_params(get_metrics_sql)
    else:
        get_metrics_sql_by_repo = f'''
        select owner, repo, github_id, github_login, sum(commit_times), sum(changed_lines), sum(diff_file_counts),sum(prs_counts) / count(github_login), sum(pr_body_length_avg) / count(github_login), sum(issues_counts) / count(github_login), 
        sum(issue_body_length_avg) / count(github_login), sum(issues_comment_count) / count(github_login), sum(issues_comment_body_length_avg) / count(github_login), sum(pr_comment_count) / count(github_login), 
        sum(pr_comment_body_length_avg) / count(github_login), sum(be_mentioned_times_in_issues) / count(github_login), sum(be_mentioned_times_in_pr) / count(github_login), 
        sum(referred_other_issues_or_prs_in_issue) / count(github_login), sum(referred_other_issues_or_prs_in_pr) / count(github_login), sum(changed_label_times_in_issues) / count(github_login), 
        sum(changed_label_times_in_prs) / count(github_login), sum(closed_issues_times) / count(github_login), sum(closed_prs_times) / count(github_login) FROM  metrics 
        where owner = {owner} and repo = {repo}
        group by (owner, repo, github_id, github_login) order by owner'''
        metrics = read_ck_server.execute_no_params(get_metrics_sql_by_repo)
    metrics_np = np.array(metrics)
    metrics_mat = metrics_np[:, 4:]
    metrics_mat = np.array(metrics_mat, dtype=float)

    factor_loadings = []
    for line in activity_factors.strip().split('\n'):
        factor_loadings.append([float(f) for f in line.split(' ')])
    factor_mat = np.mat(factor_loadings, dtype=float)

    result_mat = metrics_mat * factor_mat

    write_ck_server = CKServer(host=clickhouse_server_info["HOST"],
                               port=clickhouse_server_info["PORT"],
                               user=clickhouse_server_info["USER"],
                               password=clickhouse_server_info["PASSWD"],
                               database=clickhouse_server_info["DATABASE"])

    batch_size = 5000
    batch = []
    num_inserted = 0
    for i in range(result_mat.shape[0]):
        data_item = dict()
        data_item['owner'] = metrics[i][0]
        data_item['repo'] = metrics[i][1]
        data_item['github_id'] = int(metrics[i][2])
        data_item['github_login'] = metrics[i][3]
        matrix_a1 = result_mat[i].A1
        data_item['knowledge_sharing'] = matrix_a1[0]
        data_item['code_contribution'] = matrix_a1[1]
        data_item['issue_coordination'] = matrix_a1[2]
        data_item['progress_control'] = matrix_a1[3]
        data_item['code_tweaking'] = matrix_a1[4]
        data_item['issue_reporting'] = matrix_a1[5]
        batch.append(data_item)

        i += 1
        if i % batch_size == 0:
            write_ck_server.execute(insert_factor_sql, batch)
            batch = []
            num_inserted += batch_size
            logger.info(f'{num_inserted} activity records inserted')

    if batch:
        write_ck_server.execute(insert_factor_sql, batch)
        num_inserted += len(batch)

    logger.info(f'{num_inserted} activity records inserted')
