--每个公司有多少开发者参与贡献 
-- 六个公司的commit
-- 六个公司 issues的 发起
-- issues 回复次数
-- pr reviews comment次数
-- pr reviews approved次数
-- 包括二级目录目录的各公司分布，头部友商
-- 英伟达 关键三级目录贡献
---- meta 关键三级目录贡献
---- google 关键三级目录贡献
---- 6公司在其他关键目录中的贡献
select company,
       count()
           developer_count
from (select id, company
      from (
               select author__id      as id,
                      multiIf(splitByChar('@', commit__author__email)[2] = 'fb.com', 'meta',
                              splitByChar('@', commit__author__email)[2] = 'google.com', 'google',
                              splitByChar('@', commit__author__email)[2] = 'microsoft.com', 'microsoft',
                              splitByChar('@', commit__author__email)[2] = 'amd.com', 'amd',
                              splitByChar('@', commit__author__email)[2] = 'amazon.com', 'amazon'
                          , 'nvidia') as company
               from github_commits
               where search_key__owner = 'pytorch'
                 and author__id != 0
                 and (splitByChar('@', commit__author__email)[2] = 'fb.com' or
                      splitByChar('@', commit__author__email)[2] = 'google.com' or
                      splitByChar('@', commit__author__email)[2] = 'microsoft.com' or
                      splitByChar('@', commit__author__email)[2] = 'amd.com' or
                      splitByChar('@', commit__author__email)[2] = 'amazon.com' or
                      splitByChar('@', commit__author__email)[2] = 'nvidia.com')
               group by company, author__id
               union all
               select id, multiIf(company = 'facebook', 'meta', company) as company
               from (select id, final_company_inferred_from_company as company
                     from github_profile
                     where final_company_inferred_from_company = 'facebook'
                        or final_company_inferred_from_company = 'google'
                        or final_company_inferred_from_company = 'microsoft'
                        or final_company_inferred_from_company = 'amd'
                        or final_company_inferred_from_company = 'amazon'
                        or final_company_inferred_from_company = 'nvidia'
                     group by id, company)
               where id global in (select author__id
                                   from github_commits
                                   where search_key__owner = 'pytorch'
                                     and author__id != 0
                                   group by author__id))
      group by id, company
         )
group by company
order by developer_count desc;





--每个公司有多少开发者参与贡献
select company, count() as developer_count
from (select id, company
      from (select author__id as id, company, count()
            from (select author__id,
                         multiIf(splitByChar('@', commit__author__email)[2] =
                                 'fb.com', 'meta',
                                 splitByChar('@', commit__author__email)[2] =
                                 'google.com', 'google',
                                 splitByChar('@', commit__author__email)[2] =
                                 'microsoft.com',
                                 'microsoft',
                                 splitByChar('@', commit__author__email)[2] =
                                 'amd.com', 'amd',
                                 splitByChar('@', commit__author__email)[2] =
                                 'amazon.com', 'amazon'
                             , 'nvidia') as company
                  from github_commits
                  where search_key__owner = 'pytorch'
                    and author__id != 0
                    and (splitByChar('@', commit__author__email)[2] =
                         'fb.com' or
                         splitByChar('@', commit__author__email)[2] =
                         'google.com' or
                         splitByChar('@', commit__author__email)[2] =
                         'microsoft.com' or
                         splitByChar('@', commit__author__email)[2] =
                         'amd.com' or
                         splitByChar('@', commit__author__email)[2] =
                         'amazon.com' or
                         splitByChar('@', commit__author__email)[2] =
                         'nvidia.com'))
            group by author__id, company
            order by id, count() desc)
      limit 1 by id

      union all
      select id, if(company = 'facebook', 'meta', company)
      from (select id, final_company_inferred_from_company as company
            from github_profile
            where final_company_inferred_from_company = 'facebook'
               or final_company_inferred_from_company = 'google'
               or final_company_inferred_from_company = 'microsoft'
               or final_company_inferred_from_company = 'amd'
               or final_company_inferred_from_company = 'nvidia'
            group by id, final_company_inferred_from_company)
      where id global in (select *
                          from (select author__id
                                from github_commits
                                where search_key__owner = 'pytorch' and author__id != 0
                                group by author__id)
                          where author__id global not in (select id
                                                          from (select author__id as id, company, count()
                                                                from (select author__id,
                                                                             multiIf(
                                                                                         splitByChar('@', commit__author__email)[2] =
                                                                                         'fb.com', 'meta',
                                                                                         splitByChar('@', commit__author__email)[2] =
                                                                                         'google.com', 'google',
                                                                                         splitByChar('@', commit__author__email)[2] =
                                                                                         'microsoft.com',
                                                                                         'microsoft',
                                                                                         splitByChar('@', commit__author__email)[2] =
                                                                                         'amd.com', 'amd',
                                                                                         splitByChar('@', commit__author__email)[2] =
                                                                                         'amazon.com', 'amazon'
                                                                                 , 'nvidia') as company
                                                                      from github_commits
                                                                      where search_key__owner = 'pytorch'
                                                                        and author__id != 0
                                                                        and (splitByChar('@', commit__author__email)[2] =
                                                                             'fb.com' or
                                                                             splitByChar('@', commit__author__email)[2] =
                                                                             'google.com' or
                                                                             splitByChar('@', commit__author__email)[2] =
                                                                             'microsoft.com' or
                                                                             splitByChar('@', commit__author__email)[2] =
                                                                             'amd.com' or
                                                                             splitByChar('@', commit__author__email)[2] =
                                                                             'amazon.com' or
                                                                             splitByChar('@', commit__author__email)[2] =
                                                                             'nvidia.com'))
                                                                group by author__id, company
                                                                order by id, count() desc)
                                                          limit 1 by id)))
group by company
order by count() desc





;
-- 六个公司的commit

select company, count() as company_commit_count
from (select b.company
      from (select *
            from github_commits
            where search_key__owner = 'pytorch'
              and author__id != 0
              and sha global not in (select sha
                                     from github_commits
                                     where search_key__owner = 'pytorch'
                                       and (splitByChar('@', commit__author__email)[2] = 'fb.com' or
                                            splitByChar('@', commit__author__email)[2] = 'google.com' or
                                            splitByChar('@', commit__author__email)[2] = 'microsoft.com' or
                                            splitByChar('@', commit__author__email)[2] = 'amd.com' or
                                            splitByChar('@', commit__author__email)[2] = 'amazon.com' or
                                            splitByChar('@', commit__author__email)[2] = 'nvidia.com'))) as a global
               join (select id,company from (select author__id as id,company,count() from (select author__id,multiIf(splitByChar('@', commit__author__email)[2] =
                                                                    'fb.com', 'meta',
                                                                    splitByChar('@', commit__author__email)[2] =
                                                                    'google.com', 'google',
                                                                    splitByChar('@', commit__author__email)[2] =
                                                                    'microsoft.com',
                                                                    'microsoft',
                                                                    splitByChar('@', commit__author__email)[2] =
                                                                    'amd.com', 'amd',
                                                                    splitByChar('@', commit__author__email)[2] =
                                                                    'amazon.com', 'amazon'
                                                                , 'nvidia') as company from github_commits where search_key__owner = 'pytorch' and author__id !=0  and (splitByChar('@', commit__author__email)[2] =
                                                                  'fb.com' or
                                                                  splitByChar('@', commit__author__email)[2] =
                                                                  'google.com' or
                                                                  splitByChar('@', commit__author__email)[2] =
                                                                  'microsoft.com' or
                                                                  splitByChar('@', commit__author__email)[2] =
                                                                  'amd.com' or
                                                                  splitByChar('@', commit__author__email)[2] =
                                                                  'amazon.com' or
                                                                  splitByChar('@', commit__author__email)[2] =
                                                                  'nvidia.com')) group by author__id,company order by id, count() desc) limit 1 by id

union all
select id,if(company='facebook','meta',company) from (select id, final_company_inferred_from_company as company
from github_profile
where final_company_inferred_from_company = 'facebook'
   or final_company_inferred_from_company = 'google'
   or final_company_inferred_from_company = 'microsoft'
   or final_company_inferred_from_company = 'amd'
   or final_company_inferred_from_company = 'nvidia' group by id,final_company_inferred_from_company) where id global  in (select * from (select author__id from github_commits where search_key__owner = 'pytorch' and author__id !=0 group by author__id) where author__id global not in (select id from (select author__id as id,company,count() from (select author__id,multiIf(splitByChar('@', commit__author__email)[2] =
                                                                    'fb.com', 'meta',
                                                                    splitByChar('@', commit__author__email)[2] =
                                                                    'google.com', 'google',
                                                                    splitByChar('@', commit__author__email)[2] =
                                                                    'microsoft.com',
                                                                    'microsoft',
                                                                    splitByChar('@', commit__author__email)[2] =
                                                                    'amd.com', 'amd',
                                                                    splitByChar('@', commit__author__email)[2] =
                                                                    'amazon.com', 'amazon'
                                                                , 'nvidia') as company from github_commits where search_key__owner = 'pytorch' and author__id !=0  and (splitByChar('@', commit__author__email)[2] =
                                                                  'fb.com' or
                                                                  splitByChar('@', commit__author__email)[2] =
                                                                  'google.com' or
                                                                  splitByChar('@', commit__author__email)[2] =
                                                                  'microsoft.com' or
                                                                  splitByChar('@', commit__author__email)[2] =
                                                                  'amd.com' or
                                                                  splitByChar('@', commit__author__email)[2] =
                                                                  'amazon.com' or
                                                                  splitByChar('@', commit__author__email)[2] =
                                                                  'nvidia.com')) group by author__id,company order by id, count() desc) limit 1 by id))) as b on a.author__id = b.id

      union all


      select multiIf(splitByChar('@', commit__author__email)[2] = 'fb.com', 'meta',
                     splitByChar('@', commit__author__email)[2] = 'google.com', 'google',
                     splitByChar('@', commit__author__email)[2] = 'microsoft.com', 'microsoft',
                     splitByChar('@', commit__author__email)[2] = 'amd.com', 'amd',
                     splitByChar('@', commit__author__email)[2] = 'amazon.com', 'amazon'
                 , 'nvidia') as company
      from github_commits
      where search_key__owner = 'pytorch'
        and (splitByChar('@', commit__author__email)[2] = 'fb.com' or
             splitByChar('@', commit__author__email)[2] = 'google.com' or
             splitByChar('@', commit__author__email)[2] = 'microsoft.com' or
             splitByChar('@', commit__author__email)[2] = 'amd.com' or
             splitByChar('@', commit__author__email)[2] = 'amazon.com' or
             splitByChar('@', commit__author__email)[2] = 'nvidia.com'))
group by company
order by company_commit_count desc;



-- 六个公司 issues的 发起
select company, count() as issues_create_count
from (select id, b.company
      from (select * from github_issues where search_key__owner = 'pytorch' and pull_request__url = '') as a global
               join (select id, company
                     from (select author__id as id, company, count()
                           from (select author__id,
                                        multiIf(splitByChar('@', commit__author__email)[2] =
                                                'fb.com', 'meta',
                                                splitByChar('@', commit__author__email)[2] =
                                                'google.com', 'google',
                                                splitByChar('@', commit__author__email)[2] =
                                                'microsoft.com',
                                                'microsoft',
                                                splitByChar('@', commit__author__email)[2] =
                                                'amd.com', 'amd',
                                                splitByChar('@', commit__author__email)[2] =
                                                'amazon.com', 'amazon'
                                            , 'nvidia') as company
                                 from github_commits
                                 where search_key__owner = 'pytorch'
                                   and author__id != 0
                                   and (splitByChar('@', commit__author__email)[2] =
                                        'fb.com' or
                                        splitByChar('@', commit__author__email)[2] =
                                        'google.com' or
                                        splitByChar('@', commit__author__email)[2] =
                                        'microsoft.com' or
                                        splitByChar('@', commit__author__email)[2] =
                                        'amd.com' or
                                        splitByChar('@', commit__author__email)[2] =
                                        'amazon.com' or
                                        splitByChar('@', commit__author__email)[2] =
                                        'nvidia.com'))
                           group by author__id, company
                           order by id, count() desc)
                     limit 1 by id

                     union all
                     select id, if(company = 'facebook', 'meta', company)
                     from (select id, final_company_inferred_from_company as company
                           from github_profile
                           where final_company_inferred_from_company = 'facebook'
                              or final_company_inferred_from_company = 'google'
                              or final_company_inferred_from_company = 'microsoft'
                              or final_company_inferred_from_company = 'amd'
                              or final_company_inferred_from_company = 'nvidia'
                           group by id, final_company_inferred_from_company)
                     where id global in (select *
                                         from (select author__id
                                               from github_commits
                                               where search_key__owner = 'pytorch'
                                                 and author__id != 0
                                               group by author__id)
                                         where author__id global not in (select id
                                                                         from (select author__id as id, company, count()
                                                                               from (select author__id,
                                                                                            multiIf(
                                                                                                        splitByChar('@', commit__author__email)[2] =
                                                                                                        'fb.com',
                                                                                                        'meta',
                                                                                                        splitByChar('@', commit__author__email)[2] =
                                                                                                        'google.com',
                                                                                                        'google',
                                                                                                        splitByChar('@', commit__author__email)[2] =
                                                                                                        'microsoft.com',
                                                                                                        'microsoft',
                                                                                                        splitByChar('@', commit__author__email)[2] =
                                                                                                        'amd.com',
                                                                                                        'amd',
                                                                                                        splitByChar('@', commit__author__email)[2] =
                                                                                                        'amazon.com',
                                                                                                        'amazon'
                                                                                                , 'nvidia') as company
                                                                                     from github_commits
                                                                                     where search_key__owner = 'pytorch'
                                                                                       and author__id != 0
                                                                                       and (splitByChar('@', commit__author__email)[2] =
                                                                                            'fb.com' or
                                                                                            splitByChar('@', commit__author__email)[2] =
                                                                                            'google.com' or
                                                                                            splitByChar('@', commit__author__email)[2] =
                                                                                            'microsoft.com' or
                                                                                            splitByChar('@', commit__author__email)[2] =
                                                                                            'amd.com' or
                                                                                            splitByChar('@', commit__author__email)[2] =
                                                                                            'amazon.com' or
                                                                                            splitByChar('@', commit__author__email)[2] =
                                                                                            'nvidia.com'))
                                                                               group by author__id, company
                                                                               order by id, count() desc)
                                                                         limit 1 by id))) as b on a.user__id = b.id)
group by company
order by issues_create_count desc;






-- issues 回复次数
select company, count() as issues_comment_count
from (select b.company
      from (select a.user__id
            from (select * from github_issues_comments) as a global
                     join (select search_key__owner, search_key__repo, number
                           from github_issues
                           where search_key__owner = 'pytorch'
                             and pull_request__url = ''
                           group by search_key__owner, search_key__repo, number) as b
                          on a.search_key__owner = b.search_key__owner and a.search_key__repo = b.search_key__repo and
                             a.search_key__number = b.number) as a global
               join (select id, company
from (select author__id as id, company, count()
      from (select author__id,
                   multiIf(splitByChar('@', commit__author__email)[2] =
                           'fb.com', 'meta',
                           splitByChar('@', commit__author__email)[2] =
                           'google.com', 'google',
                           splitByChar('@', commit__author__email)[2] =
                           'microsoft.com',
                           'microsoft',
                           splitByChar('@', commit__author__email)[2] =
                           'amd.com', 'amd',
                           splitByChar('@', commit__author__email)[2] =
                           'amazon.com', 'amazon'
                       , 'nvidia') as company
            from github_commits
            where search_key__owner = 'pytorch'
              and author__id != 0
              and (splitByChar('@', commit__author__email)[2] =
                   'fb.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'google.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'microsoft.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'amd.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'amazon.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'nvidia.com'))
      group by author__id, company
      order by id, count() desc)
limit 1 by id

union all
select id, if(company = 'facebook', 'meta', company)
from (select id, final_company_inferred_from_company as company
      from github_profile
      where final_company_inferred_from_company = 'facebook'
         or final_company_inferred_from_company = 'google'
         or final_company_inferred_from_company = 'microsoft'
         or final_company_inferred_from_company = 'amd'
         or final_company_inferred_from_company = 'nvidia'
      group by id, final_company_inferred_from_company)
where id global in (select *
                    from (select author__id
                          from github_commits
                          where search_key__owner = 'pytorch' and author__id != 0
                          group by author__id)
                    where author__id global not in (select id
                                                    from (select author__id as id, company, count()
                                                          from (select author__id,
                                                                       multiIf(
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'fb.com', 'meta',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'google.com', 'google',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'microsoft.com',
                                                                                   'microsoft',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'amd.com', 'amd',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'amazon.com', 'amazon'
                                                                           , 'nvidia') as company
                                                                from github_commits
                                                                where search_key__owner = 'pytorch'
                                                                  and author__id != 0
                                                                  and (splitByChar('@', commit__author__email)[2] =
                                                                       'fb.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'google.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'microsoft.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'amd.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'amazon.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'nvidia.com'))
                                                          group by author__id, company
                                                          order by id, count() desc)
                                                    limit 1 by id))) as b on a.user__id = b.id)
group by company
order by count() desc;





select JSONExtractString(timeline_raw,'state') as state from github_issues_timeline where search_key__owner = 'pytorch' and search_key__event='reviewed' group by state;
-- pr reviews comment次数
select company, count() as commented_count
from (select b.*
      from (select JSONExtractString(JSONExtractString(timeline_raw, 'user'), 'id') as id
            from github_issues_timeline
            where search_key__owner = 'pytorch'
              and search_key__event = 'reviewed'
              and JSONExtractString(timeline_raw, 'state') = 'commented') as a
               global
               join (select id, company
from (select author__id as id, company, count()
      from (select author__id,
                   multiIf(splitByChar('@', commit__author__email)[2] =
                           'fb.com', 'meta',
                           splitByChar('@', commit__author__email)[2] =
                           'google.com', 'google',
                           splitByChar('@', commit__author__email)[2] =
                           'microsoft.com',
                           'microsoft',
                           splitByChar('@', commit__author__email)[2] =
                           'amd.com', 'amd',
                           splitByChar('@', commit__author__email)[2] =
                           'amazon.com', 'amazon'
                       , 'nvidia') as company
            from github_commits
            where search_key__owner = 'pytorch'
              and author__id != 0
              and (splitByChar('@', commit__author__email)[2] =
                   'fb.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'google.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'microsoft.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'amd.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'amazon.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'nvidia.com'))
      group by author__id, company
      order by id, count() desc)
limit 1 by id

union all
select id, if(company = 'facebook', 'meta', company)
from (select id, final_company_inferred_from_company as company
      from github_profile
      where final_company_inferred_from_company = 'facebook'
         or final_company_inferred_from_company = 'google'
         or final_company_inferred_from_company = 'microsoft'
         or final_company_inferred_from_company = 'amd'
         or final_company_inferred_from_company = 'nvidia'
      group by id, final_company_inferred_from_company)
where id global in (select *
                    from (select author__id
                          from github_commits
                          where search_key__owner = 'pytorch' and author__id != 0
                          group by author__id)
                    where author__id global not in (select id
                                                    from (select author__id as id, company, count()
                                                          from (select author__id,
                                                                       multiIf(
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'fb.com', 'meta',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'google.com', 'google',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'microsoft.com',
                                                                                   'microsoft',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'amd.com', 'amd',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'amazon.com', 'amazon'
                                                                           , 'nvidia') as company
                                                                from github_commits
                                                                where search_key__owner = 'pytorch'
                                                                  and author__id != 0
                                                                  and (splitByChar('@', commit__author__email)[2] =
                                                                       'fb.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'google.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'microsoft.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'amd.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'amazon.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'nvidia.com'))
                                                          group by author__id, company
                                                          order by id, count() desc)
                                                    limit 1 by id))) as b on a.id = toString(b.id))
group by company
order by commented_count desc






-- pr reviews approved次数
select company, count() as commented_count
from (select b.*
      from (select JSONExtractString(JSONExtractString(timeline_raw, 'user'), 'id') as id
            from github_issues_timeline
            where search_key__owner = 'pytorch'
              and search_key__event = 'reviewed'
              and JSONExtractString(timeline_raw, 'state') = 'approved') as a
               global
               join (select id, company
from (select author__id as id, company, count()
      from (select author__id,
                   multiIf(splitByChar('@', commit__author__email)[2] =
                           'fb.com', 'meta',
                           splitByChar('@', commit__author__email)[2] =
                           'google.com', 'google',
                           splitByChar('@', commit__author__email)[2] =
                           'microsoft.com',
                           'microsoft',
                           splitByChar('@', commit__author__email)[2] =
                           'amd.com', 'amd',
                           splitByChar('@', commit__author__email)[2] =
                           'amazon.com', 'amazon'
                       , 'nvidia') as company
            from github_commits
            where search_key__owner = 'pytorch'
              and author__id != 0
              and (splitByChar('@', commit__author__email)[2] =
                   'fb.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'google.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'microsoft.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'amd.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'amazon.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'nvidia.com'))
      group by author__id, company
      order by id, count() desc)
limit 1 by id

union all
select id, if(company = 'facebook', 'meta', company)
from (select id, final_company_inferred_from_company as company
      from github_profile
      where final_company_inferred_from_company = 'facebook'
         or final_company_inferred_from_company = 'google'
         or final_company_inferred_from_company = 'microsoft'
         or final_company_inferred_from_company = 'amd'
         or final_company_inferred_from_company = 'nvidia'
      group by id, final_company_inferred_from_company)
where id global in (select *
                    from (select author__id
                          from github_commits
                          where search_key__owner = 'pytorch' and author__id != 0
                          group by author__id)
                    where author__id global not in (select id
                                                    from (select author__id as id, company, count()
                                                          from (select author__id,
                                                                       multiIf(
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'fb.com', 'meta',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'google.com', 'google',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'microsoft.com',
                                                                                   'microsoft',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'amd.com', 'amd',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'amazon.com', 'amazon'
                                                                           , 'nvidia') as company
                                                                from github_commits
                                                                where search_key__owner = 'pytorch'
                                                                  and author__id != 0
                                                                  and (splitByChar('@', commit__author__email)[2] =
                                                                       'fb.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'google.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'microsoft.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'amd.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'amazon.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'nvidia.com'))
                                                          group by author__id, company
                                                          order by id, count() desc)
                                                    limit 1 by id))) as b on a.id = toString(b.id))
group by company
order by commented_count desc





-- 包括二级目录目录的各公司分布，头部友商
;
-- 一些是公司邮箱一些不是
-- 二级目录公司贡献分布
select search_key__owner, search_key__repo, in_dir, company, count() as company_alter_file_count
from (select search_key__owner,
             search_key__repo,
             in_dir,
             multiIf(splitByChar('@', author_email)[2] =
                     'fb.com', 'meta',
                     splitByChar('@', author_email)[2] =
                     'google.com', 'google',
                     splitByChar('@', author_email)[2] =
                     'microsoft.com',
                     'microsoft',
                     splitByChar('@', author_email)[2] =
                     'amd.com', 'amd',
                     splitByChar('@', author_email)[2] =
                     'amazon.com', 'amazon'
                 , 'nvidia') as company
      from gits_dir_label
      where search_key__owner = 'pytorch'
        and (splitByChar('@', author_email)[2] =
             'fb.com' or
             splitByChar('@', author_email)[2] =
             'google.com' or
             splitByChar('@', author_email)[2] =
             'microsoft.com' or
             splitByChar('@', author_email)[2] =
             'amd.com' or
             splitByChar('@', author_email)[2] =
             'amazon.com' or
             splitByChar('@', author_email)[2] =
             'nvidia.com')
        and length(splitByChar('/', in_dir)) = 3


      union all
      select search_key__owner, search_key__repo, in_dir, company
      from (select a.search_key__owner, a.search_key__repo, a.in_dir, b.author__id as id
            from (select search_key__owner, search_key__repo, in_dir, author_email
                  from gits_dir_label
                  where search_key__owner = 'pytorch'
                    and not (splitByChar('@', author_email)[2] =
                             'fb.com' or
                             splitByChar('@', author_email)[2] =
                             'google.com' or
                             splitByChar('@', author_email)[2] =
                             'microsoft.com' or
                             splitByChar('@', author_email)[2] =
                             'amd.com' or
                             splitByChar('@', author_email)[2] =
                             'amazon.com' or
                             splitByChar('@', author_email)[2] =
                             'nvidia.com')
                    and length(splitByChar('/', in_dir)) = 3) as a global
                     join (select commit__author__email, author__id
                           from github_commits
                           where search_key__owner = 'pytorch'
                             and author__id != 0
                           group by commit__author__email, author__id) as b
                          on a.author_email = b.commit__author__email) as a global
               join (select id, company
from (select author__id as id, company, count()
      from (select author__id,
                   multiIf(splitByChar('@', commit__author__email)[2] =
                           'fb.com', 'meta',
                           splitByChar('@', commit__author__email)[2] =
                           'google.com', 'google',
                           splitByChar('@', commit__author__email)[2] =
                           'microsoft.com',
                           'microsoft',
                           splitByChar('@', commit__author__email)[2] =
                           'amd.com', 'amd',
                           splitByChar('@', commit__author__email)[2] =
                           'amazon.com', 'amazon'
                       , 'nvidia') as company
            from github_commits
            where search_key__owner = 'pytorch'
              and author__id != 0
              and (splitByChar('@', commit__author__email)[2] =
                   'fb.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'google.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'microsoft.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'amd.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'amazon.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'nvidia.com'))
      group by author__id, company
      order by id, count() desc)
limit 1 by id

union all
select id, if(company = 'facebook', 'meta', company)
from (select id, final_company_inferred_from_company as company
      from github_profile
      where final_company_inferred_from_company = 'facebook'
         or final_company_inferred_from_company = 'google'
         or final_company_inferred_from_company = 'microsoft'
         or final_company_inferred_from_company = 'amd'
         or final_company_inferred_from_company = 'nvidia'
      group by id, final_company_inferred_from_company)
where id global in (select *
                    from (select author__id
                          from github_commits
                          where search_key__owner = 'pytorch' and author__id != 0
                          group by author__id)
                    where author__id global not in (select id
                                                    from (select author__id as id, company, count()
                                                          from (select author__id,
                                                                       multiIf(
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'fb.com', 'meta',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'google.com', 'google',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'microsoft.com',
                                                                                   'microsoft',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'amd.com', 'amd',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'amazon.com', 'amazon'
                                                                           , 'nvidia') as company
                                                                from github_commits
                                                                where search_key__owner = 'pytorch'
                                                                  and author__id != 0
                                                                  and (splitByChar('@', commit__author__email)[2] =
                                                                       'fb.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'google.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'microsoft.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'amd.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'amazon.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'nvidia.com'))
                                                          group by author__id, company
                                                          order by id, count() desc)
                                                    limit 1 by id))) as b on a.id = b.id)
group by search_key__owner, search_key__repo, in_dir, company

;


--pr 的发起

select company, count() as pr_create_count
from (select id, b.company
      from (select * from github_pull_requests where search_key__owner = 'pytorch') as a global
               join (select id, company
                     from (select author__id as id, company, count()
                           from (select author__id,
                                        multiIf(splitByChar('@', commit__author__email)[2] =
                                                'fb.com', 'meta',
                                                splitByChar('@', commit__author__email)[2] =
                                                'google.com', 'google',
                                                splitByChar('@', commit__author__email)[2] =
                                                'microsoft.com',
                                                'microsoft',
                                                splitByChar('@', commit__author__email)[2] =
                                                'amd.com', 'amd',
                                                splitByChar('@', commit__author__email)[2] =
                                                'amazon.com', 'amazon'
                                            , 'nvidia') as company
                                 from github_commits
                                 where search_key__owner = 'pytorch'
                                   and author__id != 0
                                   and (splitByChar('@', commit__author__email)[2] =
                                        'fb.com' or
                                        splitByChar('@', commit__author__email)[2] =
                                        'google.com' or
                                        splitByChar('@', commit__author__email)[2] =
                                        'microsoft.com' or
                                        splitByChar('@', commit__author__email)[2] =
                                        'amd.com' or
                                        splitByChar('@', commit__author__email)[2] =
                                        'amazon.com' or
                                        splitByChar('@', commit__author__email)[2] =
                                        'nvidia.com'))
                           group by author__id, company
                           order by id, count() desc)
                     limit 1 by id

                     union all
                     select id, if(company = 'facebook', 'meta', company)
                     from (select id, final_company_inferred_from_company as company
                           from github_profile
                           where final_company_inferred_from_company = 'facebook'
                              or final_company_inferred_from_company = 'google'
                              or final_company_inferred_from_company = 'microsoft'
                              or final_company_inferred_from_company = 'amd'
                              or final_company_inferred_from_company = 'nvidia'
                           group by id, final_company_inferred_from_company)
                     where id global in (select *
                                         from (select author__id
                                               from github_commits
                                               where search_key__owner = 'pytorch'
                                                 and author__id != 0
                                               group by author__id)
                                         where author__id global not in (select id
                                                                         from (select author__id as id, company, count()
                                                                               from (select author__id,
                                                                                            multiIf(
                                                                                                        splitByChar('@', commit__author__email)[2] =
                                                                                                        'fb.com',
                                                                                                        'meta',
                                                                                                        splitByChar('@', commit__author__email)[2] =
                                                                                                        'google.com',
                                                                                                        'google',
                                                                                                        splitByChar('@', commit__author__email)[2] =
                                                                                                        'microsoft.com',
                                                                                                        'microsoft',
                                                                                                        splitByChar('@', commit__author__email)[2] =
                                                                                                        'amd.com',
                                                                                                        'amd',
                                                                                                        splitByChar('@', commit__author__email)[2] =
                                                                                                        'amazon.com',
                                                                                                        'amazon'
                                                                                                , 'nvidia') as company
                                                                                     from github_commits
                                                                                     where search_key__owner = 'pytorch'
                                                                                       and author__id != 0
                                                                                       and (splitByChar('@', commit__author__email)[2] =
                                                                                            'fb.com' or
                                                                                            splitByChar('@', commit__author__email)[2] =
                                                                                            'google.com' or
                                                                                            splitByChar('@', commit__author__email)[2] =
                                                                                            'microsoft.com' or
                                                                                            splitByChar('@', commit__author__email)[2] =
                                                                                            'amd.com' or
                                                                                            splitByChar('@', commit__author__email)[2] =
                                                                                            'amazon.com' or
                                                                                            splitByChar('@', commit__author__email)[2] =
                                                                                            'nvidia.com'))
                                                                               group by author__id, company
                                                                               order by id, count() desc)
                                                                         limit 1 by id))) as b on a.user__id = b.id)
group by company
order by pr_create_count desc;









select search_key__owner, search_key__repo, in_dir, company, count() as company_alter_file_count
from (select search_key__owner,
             search_key__repo,
             in_dir,
             multiIf(splitByChar('@', author_email)[2] =
                     'fb.com', 'meta',
                     splitByChar('@', author_email)[2] =
                     'google.com', 'google',
                     splitByChar('@', author_email)[2] =
                     'microsoft.com',
                     'microsoft',
                     splitByChar('@', author_email)[2] =
                     'amd.com', 'amd',
                     splitByChar('@', author_email)[2] =
                     'amazon.com', 'amazon'
                 , 'nvidia') as company
      from gits_dir_label
      where search_key__owner = 'pytorch'
        and (splitByChar('@', author_email)[2] =
             'fb.com' or
             splitByChar('@', author_email)[2] =
             'google.com' or
             splitByChar('@', author_email)[2] =
             'microsoft.com' or
             splitByChar('@', author_email)[2] =
             'amd.com' or
             splitByChar('@', author_email)[2] =
             'amazon.com' or
             splitByChar('@', author_email)[2] =
             'nvidia.com' or
             splitByChar('@', author_email)[2] =
             'intel.com' or
             splitByChar('@', author_email)[2] =
             'samsung.com')
        and length(splitByChar('/', in_dir)) = 3


      union all
      select search_key__owner, search_key__repo, in_dir, company
      from (select a.search_key__owner, a.search_key__repo, a.in_dir, b.author__id as id
            from (select search_key__owner, search_key__repo, in_dir, author_email
                  from gits_dir_label
                  where search_key__owner = 'pytorch'
                    and not (splitByChar('@', author_email)[2] =
                             'fb.com' or
                             splitByChar('@', author_email)[2] =
                             'google.com' or
                             splitByChar('@', author_email)[2] =
                             'microsoft.com' or
                             splitByChar('@', author_email)[2] =
                             'amd.com' or
                             splitByChar('@', author_email)[2] =
                             'amazon.com' or
                             splitByChar('@', author_email)[2] =
                             'nvidia.com')
                    and length(splitByChar('/', in_dir)) = 3) as a global
                     join (select commit__author__email, author__id
                           from github_commits
                           where search_key__owner = 'pytorch'
                             and author__id != 0
                           group by commit__author__email, author__id) as b
                          on a.author_email = b.commit__author__email) as a global
               join (select id, company
from (select author__id as id, company, count()
      from (select author__id,
                   multiIf(splitByChar('@', commit__author__email)[2] =
                           'fb.com', 'meta',
                           splitByChar('@', commit__author__email)[2] =
                           'google.com', 'google',
                           splitByChar('@', commit__author__email)[2] =
                           'microsoft.com',
                           'microsoft',
                           splitByChar('@', commit__author__email)[2] =
                           'amd.com', 'amd',
                           splitByChar('@', commit__author__email)[2] =
                           'amazon.com', 'amazon'
                       , 'nvidia') as company
            from github_commits
            where search_key__owner = 'pytorch'
              and author__id != 0
              and (splitByChar('@', commit__author__email)[2] =
                   'fb.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'google.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'microsoft.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'amd.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'amazon.com' or
                   splitByChar('@', commit__author__email)[2] =
                   'nvidia.com'))
      group by author__id, company
      order by id, count() desc)
limit 1 by id

union all
select id, if(company = 'facebook', 'meta', company)
from (select id, final_company_inferred_from_company as company
      from github_profile
      where final_company_inferred_from_company = 'facebook'
         or final_company_inferred_from_company = 'google'
         or final_company_inferred_from_company = 'microsoft'
         or final_company_inferred_from_company = 'amd'
         or final_company_inferred_from_company = 'nvidia'
      group by id, final_company_inferred_from_company)
where id global in (select *
                    from (select author__id
                          from github_commits
                          where search_key__owner = 'pytorch' and author__id != 0
                          group by author__id)
                    where author__id global not in (select id
                                                    from (select author__id as id, company, count()
                                                          from (select author__id,
                                                                       multiIf(
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'fb.com', 'meta',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'google.com', 'google',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'microsoft.com',
                                                                                   'microsoft',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'amd.com', 'amd',
                                                                                   splitByChar('@', commit__author__email)[2] =
                                                                                   'amazon.com', 'amazon'
                                                                           , 'nvidia') as company
                                                                from github_commits
                                                                where search_key__owner = 'pytorch'
                                                                  and author__id != 0
                                                                  and (splitByChar('@', commit__author__email)[2] =
                                                                       'fb.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'google.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'microsoft.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'amd.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'amazon.com' or
                                                                       splitByChar('@', commit__author__email)[2] =
                                                                       'nvidia.com'))
                                                          group by author__id, company
                                                          order by id, count() desc)
                                                    limit 1 by id))) as b on a.id = b.id)
group by search_key__owner, search_key__repo, in_dir, company





;


-- 英伟达 关键三级目录贡献
select *
from (
         select search_key__owner, search_key__repo, in_dir, company, month, count() as company_alter_file_count
         from (select search_key__owner,
                      search_key__repo,
                      in_dir,
                      toYYYYMM(authored_date) as month,
                      multiIf(splitByChar('@', author_email)[2] =
                              'fb.com', 'meta',
                              splitByChar('@', author_email)[2] =
                              'google.com', 'google',
                              splitByChar('@', author_email)[2] =
                              'microsoft.com',
                              'microsoft',
                              splitByChar('@', author_email)[2] =
                              'amd.com', 'amd',
                              splitByChar('@', author_email)[2] =
                              'amazon.com', 'amazon'
                          , 'nvidia')         as company
               from gits_dir_label
               where search_key__owner = 'pytorch'
                 and (splitByChar('@', author_email)[2] =
                      'fb.com' or
                      splitByChar('@', author_email)[2] =
                      'google.com' or
                      splitByChar('@', author_email)[2] =
                      'microsoft.com' or
                      splitByChar('@', author_email)[2] =
                      'amd.com' or
                      splitByChar('@', author_email)[2] =
                      'amazon.com' or
                      splitByChar('@', author_email)[2] =
                      'nvidia.com')
                 and length(splitByChar('/', in_dir)) = 4


               union all
               select search_key__owner, search_key__repo, in_dir, month, company
               from (select a.search_key__owner, a.search_key__repo, a.in_dir, a.month, b.author__id as id
                     from (select search_key__owner,
                                  search_key__repo,
                                  in_dir,
                                  toYYYYMM(authored_date) as month,
                                  author_email
                           from gits_dir_label
                           where search_key__owner = 'pytorch'
                             and not (splitByChar('@', author_email)[2] =
                                      'fb.com' or
                                      splitByChar('@', author_email)[2] =
                                      'google.com' or
                                      splitByChar('@', author_email)[2] =
                                      'microsoft.com' or
                                      splitByChar('@', author_email)[2] =
                                      'amd.com' or
                                      splitByChar('@', author_email)[2] =
                                      'amazon.com' or
                                      splitByChar('@', author_email)[2] =
                                      'nvidia.com')
                             and length(splitByChar('/', in_dir)) = 4) as a global
                              join (select commit__author__email, author__id
                                    from github_commits
                                    where search_key__owner = 'pytorch'
                                      and author__id != 0
                                    group by commit__author__email, author__id) as b
                                   on a.author_email = b.commit__author__email) as a global
                        join (select id, company
                              from (select author__id as id, company, count()
                                    from (select author__id,
                                                 multiIf(splitByChar('@', commit__author__email)[2] =
                                                         'fb.com', 'meta',
                                                         splitByChar('@', commit__author__email)[2] =
                                                         'google.com', 'google',
                                                         splitByChar('@', commit__author__email)[2] =
                                                         'microsoft.com',
                                                         'microsoft',
                                                         splitByChar('@', commit__author__email)[2] =
                                                         'amd.com', 'amd',
                                                         splitByChar('@', commit__author__email)[2] =
                                                         'amazon.com', 'amazon'
                                                     , 'nvidia') as company
                                          from github_commits
                                          where search_key__owner = 'pytorch'
                                            and author__id != 0
                                            and (splitByChar('@', commit__author__email)[2] =
                                                 'fb.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'google.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'microsoft.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'amd.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'amazon.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'nvidia.com'))
                                    group by author__id, company
                                    order by id, count() desc)
                              limit 1 by id

                              union all
                              select id, if(company = 'facebook', 'meta', company)
                              from (select id, final_company_inferred_from_company as company
                                    from github_profile
                                    where final_company_inferred_from_company = 'facebook'
                                       or final_company_inferred_from_company = 'google'
                                       or final_company_inferred_from_company = 'microsoft'
                                       or final_company_inferred_from_company = 'amd'
                                       or final_company_inferred_from_company = 'nvidia'
                                    group by id, final_company_inferred_from_company)
                              where id global in (select *
                                                  from (select author__id
                                                        from github_commits
                                                        where search_key__owner = 'pytorch'
                                                          and author__id != 0
                                                        group by author__id)
                                                  where author__id global not in (select id
                                                                                  from (select author__id as id, company, count()
                                                                                        from (select author__id,
                                                                                                     multiIf(
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'fb.com',
                                                                                                                 'meta',
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'google.com',
                                                                                                                 'google',
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'microsoft.com',
                                                                                                                 'microsoft',
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'amd.com',
                                                                                                                 'amd',
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'amazon.com',
                                                                                                                 'amazon'
                                                                                                         ,
                                                                                                                 'nvidia') as company
                                                                                              from github_commits
                                                                                              where search_key__owner = 'pytorch'
                                                                                                and author__id != 0
                                                                                                and (splitByChar('@', commit__author__email)[2] =
                                                                                                     'fb.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'google.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'microsoft.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'amd.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'amazon.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'nvidia.com'))
                                                                                        group by author__id, company
                                                                                        order by id, count() desc)
                                                                                  limit 1 by id))) as b on a.id = b.id)
         group by search_key__owner, search_key__repo, in_dir, company, month
         )
where search_key__repo = 'pytorch'
  and company = 'nvidia'
  and in_dir global in
      ('torch/csrc/jit/', 'torch/lib/THD/', 'torch/csrc/autograd/', 'torch/nn/modules/', 'torch/csrc/distributed/',
       'aten/src/ATen/', 'aten/src/THC/', 'aten/src/THCUNN/', 'aten/src/TH/', 'torch/csrc/cuda/', 'torch/csrc/utils/',
       'torch/lib/c10d/') order by in_dir,month;







---- meta 关键三级目录贡献
select *
from (
         select search_key__owner, search_key__repo, in_dir, company, month, count() as company_alter_file_count
         from (select search_key__owner,
                      search_key__repo,
                      in_dir,
                      toYYYYMM(authored_date) as month,
                      multiIf(splitByChar('@', author_email)[2] =
                              'fb.com', 'meta',
                              splitByChar('@', author_email)[2] =
                              'google.com', 'google',
                              splitByChar('@', author_email)[2] =
                              'microsoft.com',
                              'microsoft',
                              splitByChar('@', author_email)[2] =
                              'amd.com', 'amd',
                              splitByChar('@', author_email)[2] =
                              'amazon.com', 'amazon'
                          , 'nvidia')         as company
               from gits_dir_label
               where search_key__owner = 'pytorch'
                 and (splitByChar('@', author_email)[2] =
                      'fb.com' or
                      splitByChar('@', author_email)[2] =
                      'google.com' or
                      splitByChar('@', author_email)[2] =
                      'microsoft.com' or
                      splitByChar('@', author_email)[2] =
                      'amd.com' or
                      splitByChar('@', author_email)[2] =
                      'amazon.com' or
                      splitByChar('@', author_email)[2] =
                      'nvidia.com')
                 and length(splitByChar('/', in_dir)) = 4


               union all
               select search_key__owner, search_key__repo, in_dir, month, company
               from (select a.search_key__owner, a.search_key__repo, a.in_dir, a.month, b.author__id as id
                     from (select search_key__owner,
                                  search_key__repo,
                                  in_dir,
                                  toYYYYMM(authored_date) as month,
                                  author_email
                           from gits_dir_label
                           where search_key__owner = 'pytorch'
                             and not (splitByChar('@', author_email)[2] =
                                      'fb.com' or
                                      splitByChar('@', author_email)[2] =
                                      'google.com' or
                                      splitByChar('@', author_email)[2] =
                                      'microsoft.com' or
                                      splitByChar('@', author_email)[2] =
                                      'amd.com' or
                                      splitByChar('@', author_email)[2] =
                                      'amazon.com' or
                                      splitByChar('@', author_email)[2] =
                                      'nvidia.com')
                             and length(splitByChar('/', in_dir)) = 4) as a global
                              join (select commit__author__email, author__id
                                    from github_commits
                                    where search_key__owner = 'pytorch'
                                      and author__id != 0
                                    group by commit__author__email, author__id) as b
                                   on a.author_email = b.commit__author__email) as a global
                        join (select id, company
                              from (select author__id as id, company, count()
                                    from (select author__id,
                                                 multiIf(splitByChar('@', commit__author__email)[2] =
                                                         'fb.com', 'meta',
                                                         splitByChar('@', commit__author__email)[2] =
                                                         'google.com', 'google',
                                                         splitByChar('@', commit__author__email)[2] =
                                                         'microsoft.com',
                                                         'microsoft',
                                                         splitByChar('@', commit__author__email)[2] =
                                                         'amd.com', 'amd',
                                                         splitByChar('@', commit__author__email)[2] =
                                                         'amazon.com', 'amazon'
                                                     , 'nvidia') as company
                                          from github_commits
                                          where search_key__owner = 'pytorch'
                                            and author__id != 0
                                            and (splitByChar('@', commit__author__email)[2] =
                                                 'fb.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'google.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'microsoft.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'amd.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'amazon.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'nvidia.com'))
                                    group by author__id, company
                                    order by id, count() desc)
                              limit 1 by id

                              union all
                              select id, if(company = 'facebook', 'meta', company)
                              from (select id, final_company_inferred_from_company as company
                                    from github_profile
                                    where final_company_inferred_from_company = 'facebook'
                                       or final_company_inferred_from_company = 'google'
                                       or final_company_inferred_from_company = 'microsoft'
                                       or final_company_inferred_from_company = 'amd'
                                       or final_company_inferred_from_company = 'nvidia'
                                    group by id, final_company_inferred_from_company)
                              where id global in (select *
                                                  from (select author__id
                                                        from github_commits
                                                        where search_key__owner = 'pytorch'
                                                          and author__id != 0
                                                        group by author__id)
                                                  where author__id global not in (select id
                                                                                  from (select author__id as id, company, count()
                                                                                        from (select author__id,
                                                                                                     multiIf(
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'fb.com',
                                                                                                                 'meta',
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'google.com',
                                                                                                                 'google',
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'microsoft.com',
                                                                                                                 'microsoft',
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'amd.com',
                                                                                                                 'amd',
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'amazon.com',
                                                                                                                 'amazon'
                                                                                                         ,
                                                                                                                 'nvidia') as company
                                                                                              from github_commits
                                                                                              where search_key__owner = 'pytorch'
                                                                                                and author__id != 0
                                                                                                and (splitByChar('@', commit__author__email)[2] =
                                                                                                     'fb.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'google.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'microsoft.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'amd.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'amazon.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'nvidia.com'))
                                                                                        group by author__id, company
                                                                                        order by id, count() desc)
                                                                                  limit 1 by id))) as b on a.id = b.id)
         group by search_key__owner, search_key__repo, in_dir, company, month
         )
where search_key__repo = 'pytorch'
  and company = 'meta'
  and in_dir global in
      ('torch/csrc/distributed/',
       'aten/src/TH/',
       'torch/csrc/utils/',
       'lib/Backends/CPU/',
       'py/torch_tensorrt/fx/',
       'tools/autograd/templates/',
       'torch/utils/data/',
       'aten/src/THNN/',
       'lib/Backends/Interpreter/',
       'lib/Backends/OpenCL/',
       'lib/Backends/NNPI/',
       'torch/nn/quantized/',
       'torch/csrc/cuda/',
       'tensorpipe/transport/shm/',
       'tensorpipe/transport/uv/',
       'torch/nn/parallel/',
       'torchaudio/csrc/ffmpeg/',
       'torch/csrc/profiler/',
       'lib/Optimizer/GraphOptimizer/',
       'lib/Runtime/HostManager/',
       'lib/Backends/JIT/',
       'tensorpipe/channel/cma/',
       'torch/fx/passes/',
       'haskell/fetcher/src/')
order by in_dir, month;


---- google 关键三级目录贡献
select *
from (
         select search_key__owner, search_key__repo, in_dir, company, month, count() as company_alter_file_count
         from (select search_key__owner,
                      search_key__repo,
                      in_dir,
                      toYYYYMM(authored_date) as month,
                      multiIf(splitByChar('@', author_email)[2] =
                              'fb.com', 'meta',
                              splitByChar('@', author_email)[2] =
                              'google.com', 'google',
                              splitByChar('@', author_email)[2] =
                              'microsoft.com',
                              'microsoft',
                              splitByChar('@', author_email)[2] =
                              'amd.com', 'amd',
                              splitByChar('@', author_email)[2] =
                              'amazon.com', 'amazon'
                          , 'nvidia')         as company
               from gits_dir_label
               where search_key__owner = 'pytorch'
                 and (splitByChar('@', author_email)[2] =
                      'fb.com' or
                      splitByChar('@', author_email)[2] =
                      'google.com' or
                      splitByChar('@', author_email)[2] =
                      'microsoft.com' or
                      splitByChar('@', author_email)[2] =
                      'amd.com' or
                      splitByChar('@', author_email)[2] =
                      'amazon.com' or
                      splitByChar('@', author_email)[2] =
                      'nvidia.com')
                 and length(splitByChar('/', in_dir)) = 4


               union all
               select search_key__owner, search_key__repo, in_dir, month, company
               from (select a.search_key__owner, a.search_key__repo, a.in_dir, a.month, b.author__id as id
                     from (select search_key__owner,
                                  search_key__repo,
                                  in_dir,
                                  toYYYYMM(authored_date) as month,
                                  author_email
                           from gits_dir_label
                           where search_key__owner = 'pytorch'
                             and not (splitByChar('@', author_email)[2] =
                                      'fb.com' or
                                      splitByChar('@', author_email)[2] =
                                      'google.com' or
                                      splitByChar('@', author_email)[2] =
                                      'microsoft.com' or
                                      splitByChar('@', author_email)[2] =
                                      'amd.com' or
                                      splitByChar('@', author_email)[2] =
                                      'amazon.com' or
                                      splitByChar('@', author_email)[2] =
                                      'nvidia.com')
                             and length(splitByChar('/', in_dir)) = 4) as a global
                              join (select commit__author__email, author__id
                                    from github_commits
                                    where search_key__owner = 'pytorch'
                                      and author__id != 0
                                    group by commit__author__email, author__id) as b
                                   on a.author_email = b.commit__author__email) as a global
                        join (select id, company
                              from (select author__id as id, company, count()
                                    from (select author__id,
                                                 multiIf(splitByChar('@', commit__author__email)[2] =
                                                         'fb.com', 'meta',
                                                         splitByChar('@', commit__author__email)[2] =
                                                         'google.com', 'google',
                                                         splitByChar('@', commit__author__email)[2] =
                                                         'microsoft.com',
                                                         'microsoft',
                                                         splitByChar('@', commit__author__email)[2] =
                                                         'amd.com', 'amd',
                                                         splitByChar('@', commit__author__email)[2] =
                                                         'amazon.com', 'amazon'
                                                     , 'nvidia') as company
                                          from github_commits
                                          where search_key__owner = 'pytorch'
                                            and author__id != 0
                                            and (splitByChar('@', commit__author__email)[2] =
                                                 'fb.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'google.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'microsoft.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'amd.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'amazon.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'nvidia.com'))
                                    group by author__id, company
                                    order by id, count() desc)
                              limit 1 by id

                              union all
                              select id, if(company = 'facebook', 'meta', company)
                              from (select id, final_company_inferred_from_company as company
                                    from github_profile
                                    where final_company_inferred_from_company = 'facebook'
                                       or final_company_inferred_from_company = 'google'
                                       or final_company_inferred_from_company = 'microsoft'
                                       or final_company_inferred_from_company = 'amd'
                                       or final_company_inferred_from_company = 'nvidia'
                                    group by id, final_company_inferred_from_company)
                              where id global in (select *
                                                  from (select author__id
                                                        from github_commits
                                                        where search_key__owner = 'pytorch'
                                                          and author__id != 0
                                                        group by author__id)
                                                  where author__id global not in (select id
                                                                                  from (select author__id as id, company, count()
                                                                                        from (select author__id,
                                                                                                     multiIf(
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'fb.com',
                                                                                                                 'meta',
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'google.com',
                                                                                                                 'google',
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'microsoft.com',
                                                                                                                 'microsoft',
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'amd.com',
                                                                                                                 'amd',
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'amazon.com',
                                                                                                                 'amazon'
                                                                                                         ,
                                                                                                                 'nvidia') as company
                                                                                              from github_commits
                                                                                              where search_key__owner = 'pytorch'
                                                                                                and author__id != 0
                                                                                                and (splitByChar('@', commit__author__email)[2] =
                                                                                                     'fb.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'google.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'microsoft.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'amd.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'amazon.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'nvidia.com'))
                                                                                        group by author__id, company
                                                                                        order by id, count() desc)
                                                                                  limit 1 by id))) as b on a.id = b.id)
         group by search_key__owner, search_key__repo, in_dir, company, month
         )
where search_key__repo = 'pytorch'
  and company = 'google'
  and in_dir global in
      ('torch/csrc/jit/',
       'torch/csrc/autograd/',
       'torch/lib/THD/',
       'torch/csrc/generic/',
       'torch/nn/modules/',
       'aten/src/THC/',
       'aten/src/ATen/',
       'torch_xla/csrc/ops/')
order by in_dir, month





--     英伟达 主要 3级目录贡献时间线
-- torch/csrc/jit/
-- torch/lib/THD/
-- torch/csrc/autograd/
-- torch/nn/modules/
-- torch/csrc/distributed/
--
-- aten/src/ATen/
-- aten/src/THC/
-- aten/src/THCUNN/
-- aten/src/TH/
--
-- torch/csrc/cuda/
-- torch/csrc/utils/
-- torch/lib/c10d/
--
--
-- meta 主要三级目录贡献时间线
-- torch/csrc/distributed/
-- aten/src/TH/
-- torch/csrc/utils/
-- lib/Backends/CPU/
-- py/torch_tensorrt/fx/
-- tools/autograd/templates/
-- torch/utils/data/
-- aten/src/THNN/
-- lib/Backends/Interpreter/
-- lib/Backends/OpenCL/
-- lib/Backends/NNPI/
-- torch/nn/quantized/
-- torch/csrc/cuda/
-- tensorpipe/transport/shm/
-- tensorpipe/transport/uv/
-- torch/nn/parallel/
-- torchaudio/csrc/ffmpeg/
-- torch/csrc/profiler/
-- lib/Optimizer/GraphOptimizer/
-- lib/Runtime/HostManager/
-- lib/Backends/JIT/
-- tensorpipe/channel/cma/
-- torch/fx/passes/
-- haskell/fetcher/src/
--
-- google 主要三级目录贡献时间线
--
-- torch/csrc/jit/
-- torch/csrc/autograd/
-- torch/lib/THD/
-- torch/csrc/generic/
-- torch/nn/modules/
-- aten/src/THC/
-- aten/src/ATen/
-- torch_xla/csrc/ops/


-- 这几个最重点目录 6大公司的时间线
-- torch/nn/
-- torch/nn/modules/
-- torch/csrc/jit/
-- torch/csrc/autograd/
-- torch/csrc/distributed/
-- torch/csrc/cuda/
-- torch/csrc/utils/
-- aten/
-- torch/fx
-- lib/Backends








---- 6公司在其他关键目录中的贡献
select *
from (
         select search_key__owner, search_key__repo, in_dir, company, month, count() as company_alter_file_count
         from (select search_key__owner,
                      search_key__repo,
                      in_dir,
                      toYYYYMM(authored_date) as month,
                      multiIf(splitByChar('@', author_email)[2] =
                              'fb.com', 'meta',
                              splitByChar('@', author_email)[2] =
                              'google.com', 'google',
                              splitByChar('@', author_email)[2] =
                              'microsoft.com',
                              'microsoft',
                              splitByChar('@', author_email)[2] =
                              'amd.com', 'amd',
                              splitByChar('@', author_email)[2] =
                              'amazon.com', 'amazon'
                          , 'nvidia')         as company
               from gits_dir_label
               where search_key__owner = 'pytorch'
                 and (splitByChar('@', author_email)[2] =
                      'fb.com' or
                      splitByChar('@', author_email)[2] =
                      'google.com' or
                      splitByChar('@', author_email)[2] =
                      'microsoft.com' or
                      splitByChar('@', author_email)[2] =
                      'amd.com' or
                      splitByChar('@', author_email)[2] =
                      'amazon.com' or
                      splitByChar('@', author_email)[2] =
                      'nvidia.com')


               union all
               select search_key__owner, search_key__repo, in_dir, month, company
               from (select a.search_key__owner, a.search_key__repo, a.in_dir, a.month, b.author__id as id
                     from (select search_key__owner,
                                  search_key__repo,
                                  in_dir,
                                  toYYYYMM(authored_date) as month,
                                  author_email
                           from gits_dir_label
                           where search_key__owner = 'pytorch'
                             and not (splitByChar('@', author_email)[2] =
                                      'fb.com' or
                                      splitByChar('@', author_email)[2] =
                                      'google.com' or
                                      splitByChar('@', author_email)[2] =
                                      'microsoft.com' or
                                      splitByChar('@', author_email)[2] =
                                      'amd.com' or
                                      splitByChar('@', author_email)[2] =
                                      'amazon.com' or
                                      splitByChar('@', author_email)[2] =
                                      'nvidia.com')) as a global
                              join (select commit__author__email, author__id
                                    from github_commits
                                    where search_key__owner = 'pytorch'
                                      and author__id != 0
                                    group by commit__author__email, author__id) as b
                                   on a.author_email = b.commit__author__email) as a global
                        join (select id, company
                              from (select author__id as id, company, count()
                                    from (select author__id,
                                                 multiIf(splitByChar('@', commit__author__email)[2] =
                                                         'fb.com', 'meta',
                                                         splitByChar('@', commit__author__email)[2] =
                                                         'google.com', 'google',
                                                         splitByChar('@', commit__author__email)[2] =
                                                         'microsoft.com',
                                                         'microsoft',
                                                         splitByChar('@', commit__author__email)[2] =
                                                         'amd.com', 'amd',
                                                         splitByChar('@', commit__author__email)[2] =
                                                         'amazon.com', 'amazon'
                                                     , 'nvidia') as company
                                          from github_commits
                                          where search_key__owner = 'pytorch'
                                            and author__id != 0
                                            and (splitByChar('@', commit__author__email)[2] =
                                                 'fb.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'google.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'microsoft.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'amd.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'amazon.com' or
                                                 splitByChar('@', commit__author__email)[2] =
                                                 'nvidia.com'))
                                    group by author__id, company
                                    order by id, count() desc)
                              limit 1 by id

                              union all
                              select id, if(company = 'facebook', 'meta', company)
                              from (select id, final_company_inferred_from_company as company
                                    from github_profile
                                    where final_company_inferred_from_company = 'facebook'
                                       or final_company_inferred_from_company = 'google'
                                       or final_company_inferred_from_company = 'microsoft'
                                       or final_company_inferred_from_company = 'amd'
                                       or final_company_inferred_from_company = 'nvidia'
                                    group by id, final_company_inferred_from_company)
                              where id global in (select *
                                                  from (select author__id
                                                        from github_commits
                                                        where search_key__owner = 'pytorch'
                                                          and author__id != 0
                                                        group by author__id)
                                                  where author__id global not in (select id
                                                                                  from (select author__id as id, company, count()
                                                                                        from (select author__id,
                                                                                                     multiIf(
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'fb.com',
                                                                                                                 'meta',
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'google.com',
                                                                                                                 'google',
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'microsoft.com',
                                                                                                                 'microsoft',
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'amd.com',
                                                                                                                 'amd',
                                                                                                                 splitByChar('@', commit__author__email)[2] =
                                                                                                                 'amazon.com',
                                                                                                                 'amazon'
                                                                                                         ,
                                                                                                                 'nvidia') as company
                                                                                              from github_commits
                                                                                              where search_key__owner = 'pytorch'
                                                                                                and author__id != 0
                                                                                                and (splitByChar('@', commit__author__email)[2] =
                                                                                                     'fb.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'google.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'microsoft.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'amd.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'amazon.com' or
                                                                                                     splitByChar('@', commit__author__email)[2] =
                                                                                                     'nvidia.com'))
                                                                                        group by author__id, company
                                                                                        order by id, count() desc)
                                                                                  limit 1 by id))) as b on a.id = b.id)
         group by search_key__owner, search_key__repo, in_dir, company, month
         )
where search_key__repo = 'pytorch'
  and in_dir global in
      ('torch/nn/',
'torch/nn/modules/',
'torch/csrc/jit/',
'torch/csrc/autograd/',
'torch/csrc/distributed/',
'torch/csrc/cuda/',
'torch/csrc/utils/',
'aten/',
'torch/fx/',
'lib/Backends/')
order by in_dir,company, month





