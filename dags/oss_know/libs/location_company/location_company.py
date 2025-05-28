import datetime
import time

from oss_know.libs.base_dict.location_map import email_domain_company_map_list, country_or_region_area_map
from oss_know.libs.util.log import logger

from oss_know.libs.util.clickhouse_driver import CKServer




class LoginEmailLocationCompany(object):

    def __init__(self, clickhouse_server_info):
        self.ck_client = CKServer(host=clickhouse_server_info["HOST"],
                                  port=clickhouse_server_info["PORT"],
                                  user=clickhouse_server_info["USER"],
                                  password=clickhouse_server_info["PASSWD"],
                                  database=clickhouse_server_info["DATABASE"])
        self.bulk_data = []
        self.bulk_login_location_data = []
        self.bulk_email_login_data = []

    def get_email_location(self):
        """
        获取某邮箱使用者的位置信息
        """


        sql_ = """
        select email,
        location,
       if(splitByChar('@', email)[2] global in (
                                                '163.com',
                                                '126.com',
                                                'qq.com',
                                                'foxmail.com',
                                                'streamcomputing.com',
                                                'loongson.cn',
                                                'iscas.ac.cn'
           ), 1, 0) is_chinese_email_address
from (
      select email, location
      from (select email,
                   groupArray((region, commit_count)) as                            region_commit_count_rank
                    ,
--     region_commit_count_rank[1] as a,
--     a.1,
                   if(length(region_commit_count_rank) > 1 and
                      (region_commit_count_rank[1].1 = '0时区' or region_commit_count_rank[1].1 = '其他'),
                      region_commit_count_rank[2].1, region_commit_count_rank[1].1) location
            from (select email, region, sum(commit_count) as commit_count
                  from (select email,
                               tz,
                               multiIf(tz in (8), '中国',
                                       tz in (-1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12),
                                       '北美',
                                       tz in (1, 2, 3, 4), '欧洲', tz in (5), '印度', tz in (10), '澳洲',
                                       tz in (9), '日韩',
                                       tz in (0), '0时区', '其他') as region,
                               sum(commit_count)                   as commit_count
                        from (select email, tz, count() as commit_count
                              from (select if((startsWith(email, '"') and endsWith(email, '"')) or (startsWith(email, '”') and endsWith(email, '”'))  or (startsWith(email, '¨') and endsWith(email, '¨')),
                                              substring(email, 2, length(email) - 2), email) as email,
                                           tz
                                    from (select argMax(author_email, search_key__updated_at)  as email,
                                                 argMax(author_tz, search_key__updated_at)     as tz,
                                                 argMax(authored_date, search_key__updated_at) as commit_date
                                          from (select *
                                                from gits
                                                where author_email global not in (select email
                                                                                  from (select email, location
                                                                                        from (select * from github_login_location) as a global
                                                                                                 join (select login, email
                                                                                                       from (select author__login as login, commit__author__email as email
                                                                                                             from github_commits
                                                                                                             where author__login != ''
                                                                                                             group by author__login, commit__author__email
                                                                                                             union all
                                                                                                             select committer__login as login, commit__committer__email as email
                                                                                                             from github_commits
                                                                                                             where committer__login != ''
                                                                                                             group by committer__login, commit__committer__email)
                                                                                                       group by login, email) as b
                                                                                                      on a.github_login = b.login)
                                                                                  group by email))
--                   where
                                          group by hexsha
                                          union all
                                          select argMax(committer_email, search_key__updated_at) as email,
                                                 argMax(committer_tz, search_key__updated_at)    as tz,
                                                 argMax(committed_date, search_key__updated_at)  as commit_date
                                          from (select *
                                                from gits
                                                where committer_email global not in (select email
                                                                                     from (select email, location
                                                                                           from (select * from github_login_location) as a global
                                                                                                    join (select login, email
                                                                                                          from (select author__login as login, commit__author__email as email
                                                                                                                from github_commits
                                                                                                                where author__login != ''
                                                                                                                group by author__login, commit__author__email
                                                                                                                union all
                                                                                                                select committer__login as login, commit__committer__email as email
                                                                                                                from github_commits
                                                                                                                where committer__login != ''
                                                                                                                group by committer__login, commit__committer__email)
                                                                                                          group by login, email) as b
                                                                                                         on a.github_login = b.login)
                                                                                     group by email))
--                   where
                                          group by hexsha
                                             )
                                    where email like '%@%'
                                    group by email, tz, commit_date)
                              group by email, tz
                                 )
                        group by email, tz
                        order by email, commit_count desc)
                  group by email, region
                  order by email, commit_count desc)
            group by email
               ))
        """
        sql_2 = """
        select email,
       location,
       if(splitByChar('@', email)[2] global in (
                                                '163.com',
                                                '126.com',
                                                'qq.com',
                                                'foxmail.com',
                                                'streamcomputing.com',
                                                'loongson.cn',
                                                'iscas.ac.cn'
           ), 1, 0) is_chinese_email_address
from (

    select email, location
      from (select * from github_login_location) as a global
               join (select login, email
                     from (select author__login as login, commit__author__email as email
                           from github_commits
                           where author__login != ''
                           group by author__login, commit__author__email
                           union all
                           select committer__login as login, commit__committer__email as email
                           from github_commits
                           where committer__login != ''
                           group by committer__login, commit__committer__email)
                     group by login, email) as b on a.github_login = b.login
)
        """
        git_email_locations = self.ck_client.execute_no_params(sql_)
        github_login_email_locations = self.ck_client.execute_no_params(sql_2)
        timestamp = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)

        def email_locations_to_dict(email_locations):
            for email_location in email_locations:
                self.bulk_email_login_data.append({
                    "inserted_at": timestamp,
                    "email": email_location[0],
                    "location": email_location[1],
                    "is_chinese_email_address": email_location[2]
                })

        email_locations_to_dict(git_email_locations)
        email_locations_to_dict(github_login_email_locations)

        self.ck_client.execute_no_params("truncate table github_email_location_local on cluster replicated")
        time.sleep(10)
        self.insert_into_ck(self.bulk_email_login_data, 'github_email_location')

    def get_email_company_range(self):
        """
        获取某email使用者在不同时间段所属的公司
        :return:
        """
        sql_ = """
        select email, company, start_at, end_at
from (select * from github_login_company_time_range_v1) as a global
         join (select login, email
               from (select author__login as login, commit__author__email as email
                     from github_commits
                     where author__login != ''
                     group by author__login, commit__author__email
                     union all
                     select committer__login as login, commit__committer__email as email
                     from github_commits
                     where committer__login != ''
                     group by committer__login, commit__committer__email)
               group by login, email) as b on a.github_login = b.login
union all
select email, company, min(month) as start_at, max(month) as end_at
from (select email, company, toYYYYMM(commit_date) as month
      from (select email, commit_date, splitByChar('@', email)[2] as email_domain
            from (select committer_email as email, committed_date as commit_date
                  from gits
                  where committer_email global not in (select email
                                                       from (select email, company, start_at, end_at
                                                             from (select * from github_login_company_time_range_v1) as a global
                                                                      join (select login, email
                                                                            from (select author__login as login, commit__author__email as email
                                                                                  from github_commits
                                                                                  where author__login != ''
                                                                                  group by author__login, commit__author__email
                                                                                  union all
                                                                                  select committer__login as login, commit__committer__email as email
                                                                                  from github_commits
                                                                                  where committer__login != ''
                                                                                  group by committer__login, commit__committer__email)
                                                                            group by login, email) as b
                                                                           on a.github_login = b.login)
                                                       group by email)
                  group by hexsha, committer_email, committed_date
                  union all
                  select author_email as email, authored_date as commit_date
                  from gits
                  where author_email global not in (select email
                                                    from (select email, company, start_at, end_at
                                                          from (select * from github_login_company_time_range_v1) as a global
                                                                   join (select login, email
                                                                         from (select author__login as login, commit__author__email as email
                                                                               from github_commits
                                                                               where author__login != ''
                                                                               group by author__login, commit__author__email
                                                                               union all
                                                                               select committer__login as login, commit__committer__email as email
                                                                               from github_commits
                                                                               where committer__login != ''
                                                                               group by committer__login, commit__committer__email)
                                                                         group by login, email) as b
                                                                        on a.github_login = b.login)
                                                    group by email)
                  group by hexsha, author_email, authored_date)
            group by email, commit_date) as a global
               join (select * from company_email_map) as b on a.email_domain = b.email_domain)
group by email, company
        """
        results = self.ck_client.execute_no_params(sql_)
        bulk_data = []
        for result in results:
            bulk_data.append({
                "inserted_at": int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000),
                "email": result[0],
                "company": result[1],
                "start_at": result[2],
                "end_at": result[3]
            })
        self.ck_client.execute_no_params("truncate table email_company_time_range_local on cluster replicated")
        self.insert_into_ck(bulk_data, "email_company_time_range")

    def get_login_company_time_range(self):
        """
        推出每个开发者在不同时间段属于哪个公司（结合工作履历）
        :return:
        """
        work_experience_website_sql = """
        -- 通过工作网站履历  数据找出开发者公司时间段
select github_login,
       experience_login,
       company,
       toInt64(start_at),
       toInt64(end_et),
       position
from (select github_login,
             experience_login,
             replaceAll(b.experience_img, ' logo', '') as         remove_logo,
             lower(if(remove_logo = '' and
                      company_or_total_years_text like '% · %',
                      splitByString(' · ', company_or_total_years_text)[1],
                      remove_logo))                    as         company_1,
             -- 对某些公司名称的特殊处理 （如改名）
             if(company_1 like '%facebook', 'meta', company_1)    company,
             experience_img,
             experience_title,
             station,
             company_or_total_years_text,
             experience_time_start_to_end,
             position,
             splitByString('--', experience_time_start_to_end)[1] start_at,
             splitByString('--', experience_time_start_to_end)[2] end_et
      --        ,
--        if(start < 2100, toInt64(concat(toString(start), '01')), start)          cleand_start,
--        if(end < 2100, toInt64(concat(toString(end), '01')), end)                cleand_end

      from (

               -- 工作履历网站账号的映射关系 github_login _account
               select github_login, experience_login from github_experience_login_map
               group by github_login, experience_login
               ) as a global
               join (select *
                     from developer_work_experience_website_info
                     where experience_time_start_to_end != '') as b
                    on a.experience_login = b.web_login)
        """
        email_domain_company = """
        
-- login 公司 通过gits log邮箱后缀推出的在一个公司的起始时间和结束时间
    select login,
           company,
           min(start_at) as start_at,
           max(end_at)   as end_at
    from (select a.*, b.company
          from (
                   --todo 一个login的一个邮箱后缀的起始时间和结束时间 有瑕疵 需要再细化推断
                   select login,
                          splitByChar('@', email)[2] as email_domain,
                          min(month)                 as start_at,
                          max(month)                 as end_at
                   from (select commits.author_github_login as login,
                                commits.author_email        as email,
                                toInt64(concat(
                                        splitByChar('-', substring(`commits.author_date`, 1, 10))[1],
                                        splitByChar('-', substring(`commits.author_date`, 1, 10))[2]
                                    --                                 ,
--                                     splitByChar('-', substring(`commits.author_date`, 1, 10))[3]
                                    ))                      as month
                         from nvidia_contributor_pr_v3 array join commits.author_github_login, commits.author_email, `commits.author_date`
                         where login != ''

                         union all
                         select author__login,
                                commit__author__email,
                                toYYYYMM(commit__author__date) as month
                         from github_commits
                         where author__login != ''

                           and author__login global not in
                               (select robot_login_email from robot_login_email)
                           and author__login global not in
                               (select robot_login_email
                                from robot_login_email)
                            )
                   group by login, email_domain) as a global
                   join company_email_map as b on a.email_domain = b.email_domain)
    group by login, company
        """
        work_experience_website_company_results = self.ck_client.execute_no_params(work_experience_website_sql)
        git_email_company_results = self.ck_client.execute_no_params(email_domain_company)
        map_ = {}

        # 不同格式的相同公司名字合并
        def different_format_company_merge(company):
            if company == 'red hat':
                return 'redhat'
            return company

        for result in work_experience_website_company_results:
            login = result[0]

            company = result[2]
            if company == '':
                continue
            start_at = result[3]
            end_at = result[4]
            company_time_interval = map_.get(login, [])
            company_time_interval.append((company, start_at, end_at))
            map_[login] = company_time_interval

        for result in git_email_company_results:
            login = result[0]

            company = result[1]
            if company == '':
                continue
            start_at = result[2]
            end_at = result[3]
            company_time_interval = map_.get(login, [])
            company_time_interval.append((company, start_at, end_at))
            map_[login] = company_time_interval

        for login in map_:
            company_times = map_.get(login)
            company_map = {}
            for company_time in company_times:
                time_interval = company_map.get(company_time[0], [])
                time_interval.append((company_time[1], company_time[2]))
                company_map[company_time[0]] = time_interval
            for company in company_map:
                time_ranges = company_map.get(company)
                time_ranges = sorted(time_ranges, key=lambda x: int(x[0]))
                company = different_format_company_merge(company)
                start = time_ranges[0][0]
                end = time_ranges[0][1]
                results = []
                timestamp = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)

                for i in range(1, len(time_ranges)):

                    if int(time_ranges[i][0]) <= int(time_ranges[i - 1][1]) or int(time_ranges[i][0]) - int(
                            time_ranges[i - 1][1]) == 1:
                        end = time_ranges[i][1]

                    else:

                        results.append((start, end))
                        self.bulk_data.append({
                            "inserted_at": timestamp,
                            "github_login": login,
                            "company": company,
                            "start_at": start,
                            "end_at": end
                        })

                        start = time_ranges[i][0]
                        end = time_ranges[i][1]
                results.append((start, end))
                self.bulk_data.append({
                    "inserted_at": timestamp,
                    "github_login": login,
                    "company": company,
                    "start_at": start,
                    "end_at": end
                })

        self.ck_client.execute_no_params(
            "truncate table github_login_company_time_range_v1_local on cluster replicated")
        time.sleep(5)
        self.insert_into_ck(self.bulk_data, 'github_login_company_time_range_v1')

    def company_email_domain_map(self):
        """
        company_email_domain映射关系表生成
        :return:
        """
        bulk_data = []
        timestamp = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        for email_domain, company in email_domain_company_map_list:
            bulk_data.append({
                "updated_at": timestamp,
                "email_domain": email_domain,
                "company": company
            })
        self.ck_client.execute_no_params("truncate table company_email_map_local on cluster replicated")
        self.insert_into_ck(bulk_data, "company_email_map")

    def get_github_login_location(self):
        """
        获取github login 地理位置
        通过tz github_profile work_experience_website 三方面综合推断
        :return:
        """

        sql_ = """
        select github_login,
               inferred_area
        from (
        -- 时区 中国邮箱 
         select if(b.login != '', b.login, a.github_login) as github_login,
                --
                multiIf(a.inferred_area != '' and a.inferred_area = b.inferred_from_location__country, b.area,
                        a.inferred_area = '0时区' and b.inferred_from_location__country != '', b.area,
                        a.inferred_area != '' and a.inferred_area != b.inferred_from_location__country,
                        a.inferred_area,
                        b.area)                            as inferred_area

         from (select github_login, inferred_area, main_tz_area, location

               from github_id_main_tz_map_v2) as a global
                  full join (
             -- profile
             select login, inferred_from_location__country, area
             from (select login,
                          inferred_from_location__country
                   from github_profile
                   where inferred_from_location__country != ''
                      ) as a global
                      left join (
                 select country_or_region, area from country_tz_region_map
                 ) as b on a.inferred_from_location__country = b.country_or_region
             group by login, inferred_from_location__country, area) as b
                            on a.github_login = b.login)
group by github_login, inferred_area
        """

        tz_profile_location = self.ck_client.execute_no_params(sql_)
        login_location_month_count = self.work_experience_website_location_time_range()
        login_location = {}

        for result in tz_profile_location:
            login = result[0]
            inferred_area = result[1]
            if inferred_area == '其他' or inferred_area == '0时区':
                location_info = login_location_month_count.get(login)
                if login_location_month_count.get(login) is not None:
                    inferred_area = location_info['location']

            login_location[login] = inferred_area

            self.bulk_login_location_data.append({
                "inserted_at": int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000),
                "github_login": login,
                "location": inferred_area
            })

        for login in login_location_month_count:
            if login_location.get(login) is None:

                self.bulk_login_location_data.append(
                    {
                        "inserted_at": int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000),
                        "github_login": login,
                        "location": login_location_month_count.get(login)['location']
                    }
                )

        self.ck_client.execute_no_params("truncate table github_login_location_local on cluster replicated")
        time.sleep(5)
        self.insert_into_ck(self.bulk_login_location_data, 'github_login_location')

    def work_experience_website_location_time_range(self):
        """
        合并work_experience_website 重叠时间段位置信息
        :return:
        """
        sql_ = """
        select b.github_login,month_interval,position,experience_time_start_to_end,inferred_from_location__country,area from (select a.*, b.area
from (
         select user_experience_login,
                month_interval,
                position,
                
                experience_time_start_to_end,
                inferred_from_location__country
         from (select user_experience_login,
                      position,
                      month_interval,
                      cleaned_location_keywords,
                      experience_time_start_to_end,
                      inferred_from_location__country
               from (select cleaned_location_keywords,
                            month_interval,
                            position,
                            user_experience_login,
                            experience_time_start_to_end
                     from (select user_experience_login,
                                  cleaned_location_keywords,
                                  position,

                                  experience_time_start_to_end,
                                  month_interval
                           from (
                                    select user_experience_login,
                                           position,
                                           experience_time_start_to_end,
                                           cleaned_location_keywords,
                                           month_interval
                                    from developer_work_experience_website_info
                                    where data_type != 'edu'
                                      and position != ''
--            and lower(position)  '%Remote%'
                                    group by user_experience_login, position,
                                             experience_time_start_to_end,
                                             cleaned_location_keywords, month_interval
                                    ))
                              array join cleaned_location_keywords) as a global
                        left join (select location, inferred_from_location__country
                                   from github_profile
                                   where location != ''
                                     and inferred_from_location__country != ''
                                   group by location, inferred_from_location__country) as b
                                  on lower(a.cleaned_location_keywords) = lower(b.location) or
                                     lower(position) = lower(b.location)
               group by user_experience_login, position, cleaned_location_keywords,
                        inferred_from_location__country,
                        experience_time_start_to_end, month_interval
               order by user_experience_login, position)
         where inferred_from_location__country != ''
--and user_experience_login = 'U007D'
         group by user_experience_login, position, month_interval, experience_time_start_to_end,
                  inferred_from_location__country
         order by user_experience_login) as a global
         left join (select * from country_tz_region_map) as b on a.inferred_from_location__country = b.country_or_region) as a global join
    (select * from github_experience_login_map
) as b on a.user_experience_login = b.experience_login
        """

        login_location_map = {

        }
        merged_month_range = self.ck_client.execute_no_params(sql_)
        for result in merged_month_range:
            login = result[0]
            location = result[-1]
            if not location:
                location = result[-2]
            start = result[-3].split('--')[0]
            end = result[-3].split('--')[1]
            if login_location_map.get(login):
                if login_location_map.get(login).get(location):
                    login_location_map.get(login).get(location).append((start, end))
                else:
                    login_location_map.get(login)[location] = [(start, end)]
            else:
                login_location_map[login] = {
                    location: [(start, end)]
                }
        login_location_month_count = {}
        # 按照时间数组的第一个元素进行排列
        for login in login_location_map:
            max_month_range = 0
            max_month_range_location = ''
            for location in login_location_map[login]:
                time_ranges = login_location_map[login][location]
                time_ranges = sorted(time_ranges, key=lambda x: int(x[0]))
                # 将每个地区重合的时间区间合并
                start = time_ranges[0][0]
                end = time_ranges[0][1]
                merged_month_range = []
                for i in range(1, len(time_ranges)):
                    if (int(time_ranges[i][0]) <= int(time_ranges[i - 1][1]) or
                            int(time_ranges[i][0]) - int(time_ranges[i - 1][1]) == 1):
                        pass
                    else:
                        merged_month_range.append((start, end))
                        start = time_ranges[i][0]
                    end = time_ranges[i][1]
                merged_month_range.append((start, end))
                month_range = 0
                for time_range in merged_month_range:
                    start_at = time_range[0]
                    end_at = time_range[1]
                    start_month = int(start_at[-2:])
                    end_month = int(end_at[-2:])
                    start_year = int(start_at[:4])
                    end_year = int(end_at[:4])
                    if int(end_month) < int(start_month):
                        end_year -= 1
                        end_month += 12

                    month_range += (end_year - start_year) * 12 + end_month - start_month
                if month_range > max_month_range:
                    max_month_range = month_range
                    max_month_range_location = location

                login_location_map[login][location] = month_range

                timestamp = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)

            login_location_month_count[login] = {
                "location": max_month_range_location,
                "month_count": max_month_range
            }

        return login_location_month_count

    def robot_login_email(self):
        self.ck_client.execute_no_params('truncate table robot_login_email_local on cluster replicated')

        sql_ = """
        insert into table robot_login_email
        select github_login as robot_login_email,toUnixTimestamp(now())
from (select github_login
      from code_owners
      where github_login global in
            ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot', 'k8s-triage-robot', 'bors', 'rustbot',
                'rust-log-analyzer', 'rust-timer', 'llvmbot', 'github-actions[bot]','facebook-github-bot', 'pytorchmergebot', 'pytorchupdatebot', 'dependabot[bot]',
                'tensorflower-gardener']
         or (github_login like '%-bot%'
          or github_login like '%[bot]%'
          or github_login like '%sig-%'
          or github_login like '%release-%'
          or github_login like '%feature-%'
          or github_login like '%api-%'
          or github_login like '%dep-%'
          or github_login like '%build-%'
          or github_login like '%-approvers'
          or github_login like '%-maintainers'
          or github_login like '%-reviewers')
      group by github_login
      union all
      select github_login
      from (select committer__login as github_login
            from github_commits
            where committer__login != ''
            group by github_login
            union all
            select author__login as github_login
            from github_commits
            where author__login != ''
            group by github_login
               )
      where github_login global in
            ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot',
                'k8s-triage-robot', 'bors', 'rustbot',
                'rust-log-analyzer',
                'rust-timer',
                'llvmbot',
                'github-actions[bot]',
                'facebook-github-bot',
                'pytorchmergebot',
                'pytorchupdatebot',
                'dependabot[bot]',
                'tensorflower-gardener']
         or (github_login like '%-bot%'
          or github_login like '%[bot]%'
          or github_login like '%sig-%'
          or github_login like '%release-%'
          or github_login like '%feature-%'
          or github_login like '%api-%'
          or github_login like '%dep-%'
          or github_login like '%build-%'
          or github_login like '%-approvers'
          or github_login like '%-maintainers'
          or github_login like '%-reviewers')
      group by github_login

      union all
      select email
      from (select committer__login as github_login, commit__committer__email as email
            from github_commits
            where committer__login != ''
            group by github_login, email
            union all
            select author__login as github_login, commit__author__email as email
            from github_commits
            where author__login != ''
            group by github_login, email
               )
      where github_login global in
            ['k8s-ci-robot', 'k8s-triage-robot', 'k8s-ci-robot',
                'k8s-triage-robot', 'bors', 'rustbot',
                'rust-log-analyzer',
                'rust-timer',
                'llvmbot',
                'github-actions[bot]',
                'facebook-github-bot',
                'pytorchmergebot',
                'pytorchupdatebot',
                'dependabot[bot]',
                'tensorflower-gardener']
         or (github_login like '%-bot%'
          or github_login like '%[bot]%'
          or github_login like '%sig-%'
          or github_login like '%release-%'
          or github_login like '%feature-%'
          or github_login like '%api-%'
          or github_login like '%dep-%'
          or github_login like '%build-%'
          or github_login like '%-approvers'
          or github_login like '%-maintainers'
          or github_login like '%-reviewers')
      group by email)
group by github_login
        
        """
        self.ck_client.execute_no_params(sql_)

    def get_github_id_tz_region_map(self):
        # 全量数据的 id 和 主时区映射推断
        self.ck_client.execute_no_params("truncate table github_id_main_tz_map_v2_local on cluster replicated")
        id_tz_region_sql = """
        select a.*, b.github_login
from (select a.*, b.is_chinese_email
      from (select a.*, area
            from (select a.*, raw_location, inferred_with_cctld_location, city, province, cctld, company_inferred
                  from (select github_id, groupArray((emails, region, commit_count)) as tz_area_map
                        from (select github_id,
                                     region,
                                     groupArray(email) as emails,
                                     sum(commit_count) as commit_count
                              from (select email, region, sum(commit_count) as commit_count
                                    from (select email,
                                                 tz,
                                                 multiIf(tz in (8), '中国',
                                                         tz in (-1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12),
                                                         '北美',
                                                         tz in (1, 2, 3, 4), '欧洲', tz in (5), '印度', tz in (10),
                                                         '澳洲',
                                                         tz in (9), '日韩',
                                                         tz in (0), '0时区', '其他') as region,
                                                 count()                             as commit_count
                                          from (select if((startsWith(email, '"') and endsWith(email, '"')) or
                                                          (startsWith(email, '”') and endsWith(email, '”')) or
                                                          (startsWith(email, '¨') and endsWith(email, '¨')),
                                                          substring(email, 2, length(email) - 2), email) as email,
                                                       tz
                                                from (select argMax(author_email, search_key__updated_at)  as email,
                                                             argMax(author_tz, search_key__updated_at)     as tz,
                                                             argMax(authored_date, search_key__updated_at) as commit_date
                                                      from (select * from gits)
--                   where
                                                      group by hexsha
                                                      union all
                                                      select argMax(committer_email, search_key__updated_at) as email,
                                                             argMax(committer_tz, search_key__updated_at)    as tz,
                                                             argMax(committed_date, search_key__updated_at)  as commit_date
                                                      from (select * from gits)
--                   where
                                                      group by hexsha
                                                         )
                                                where email like '%@%'
                                                group by email, tz, commit_date)
                                          group by email, tz

                                          order by email, commit_count desc)
                                    group by email, region
                                    order by email, commit_count desc) as a global
                                       join (select email, github_id
                                             from (select argMax(commit__author__email, search_key__updated_at) as email,
                                                          argMax(author__id, search_key__updated_at)            as github_id
                                                   from github_commits
                                                   where author__id != 0
                                                   group by sha
                                                   union all
                                                   select argMax(commit__committer__email, search_key__updated_at) as email,
                                                          argMax(committer__id, search_key__updated_at)            as github_id
                                                   from github_commits
                                                   where committer__id != 0
                                                   group by sha
                                                      )
                                             group by email, github_id) as b on a.email = b.email
                              where github_id != 0
                              group by github_id, region
                              order by github_id, commit_count desc)
                        group by github_id) as a global
                           left join (select id,
                                             raw_location,
                                             if(location_inferred = '', cctld, location_inferred) as inferred_with_cctld_location,
                                             city,
                                             province,
                                             cctld,
                                             company_inferred
                                      from (select id,
                                                   argMax(location, updated_at)                            as raw_location,
                                                   argMax(country_inferred_from_location, updated_at)      as location_inferred,
                                                   argMax(inferred_from_location__locality, updated_at)    as city,
                                                   argMax(inferred_from_location__administrative_area_level_1,
                                                          updated_at)                                      as province,
                                                   argMax(country_inferred_from_email_cctld, updated_at)   as cctld,
                                                   argMax(company, updated_at)                             as raw_company,
                                                   argMax(final_company_inferred_from_company, updated_at) as final_company_inferred_from_company,
                                                   if(final_company_inferred_from_company = '', raw_company,
                                                      final_company_inferred_from_company)                 as company_inferred

                                            from github_profile
                                            group by id)) as b
                                     on a.github_id = b.id) as a global
                     left join (select country_or_region, argMax(area, update_at) area
                                from country_tz_region_map
                                group by country_or_region
                ) as b on a.inferred_with_cctld_location = b.country_or_region
               ) as a global
               left join (select github_id, is_chinese_email
                          from (select *, 1 as is_chinese_email
                                from (select github_id,
                                             email,
                                             splitByChar('@', email)[2] as email_domain

                                      from (select argMax(commit__author__email, search_key__updated_at) as email,
                                                   argMax(author__id, search_key__updated_at)            as github_id
                                            from github_commits
                                            where author__id != 0
                                            group by sha
                                            union all
                                            select argMax(commit__committer__email, search_key__updated_at) as email,
                                                   argMax(committer__id, search_key__updated_at)            as github_id
                                            from github_commits
                                            where committer__id != 0
                                            group by sha)
                                      group by github_id, email)
                                where email_domain global in (
                                                              '163.com',
                                                              '126.com',
                                                              'qq.com',
                                                              'foxmail.com',
                                                              'streamcomputing.com',
                                                              'loongson.cn',
                                                              'iscas.ac.cn'
                                    ))
                          group by github_id, is_chinese_email) as b on a.github_id = b.github_id) as a global
         left join (
    select github_id, argMax(github_login, max_date) as github_login
    from (select author__id                                  as github_id,
                 argMax(author__login, commit__author__date) as github_login,
                 max(commit__author__date)                   as max_date
          from github_commits
          where author__login != ''
          group by author__id
          union all
          select committer__id                                     as github_id,
                 argMax(committer__login, commit__committer__date) as github_login,
                 max(commit__committer__date)                      as max_date
          from github_commits
          where committer__login != ''
          group by committer__id)
    group by github_id
    ) as b on a.github_id = b.github_id
        """

        bulk_data = []
        update_at = datetime.datetime.now()
        # 13位时间戳
        update_at_timestamp = int(datetime.datetime.now().timestamp() * 1000)
        id_tz_region_resp = self.ck_client.execute_no_params(id_tz_region_sql)
        insert_size = 50000
        for result in id_tz_region_resp:
            github_id = result[0]
            timezones = result[1]
            area = result[3]
            raw_location = result[2]
            location = result[3]
            is_chinese_email = result[-2]
            city = result[4]
            province = result[5]
            cctld = result[6]
            company = result[7]
            github_login = result[-1]

            # area 推断
            """

            """
            if area:
                if area:
                    area = country_or_region_area_map.get(area) if country_or_region_area_map.get(area) else area

            if timezones[0][1] == '0时区' and len(timezones) > 1:
                main_timezone = timezones[1][1]
                if area:
                    inferred_area = area
                else:
                    inferred_area = main_timezone
            elif timezones[0][1] == '0时区' and len(timezones) == 1:
                main_timezone = '0时区'
                if area:
                    inferred_area = area
                else:
                    inferred_area = main_timezone
            else:
                main_timezone = timezones[0][1]
                if area:
                    inferred_area = area
                else:
                    inferred_area = main_timezone

            # 中国邮箱修正(不包含公司邮箱修正)
            if is_chinese_email == 1:
                flag = 0
                # for tz in timezones:
                #     if tz[1] == '中国':
                #         inferred_area = '中国'
                if area:
                    if area not in ('新加坡', '中国台湾', '澳洲', '东南亚', '东亚'):
                        inferred_area = '中国'
                else:
                    inferred_area = '中国'

            # if main_timezone == '中国' and is_chinese_email == 1:
            #     inferred_area = '中国'
            if main_timezone == '中国' and is_chinese_email == 0 and area != '中国':
                #
                if area:
                    inferred_area = area
                else:
                    inferred_area = main_timezone


            bulk_data.append(
                {"update_at": update_at,
                 "update_at_timestamp": update_at_timestamp, "github_id": github_id,
                 "main_tz_area": main_timezone,
                 "inferred_area": inferred_area,
                 "location": location,
                 "top_n_tz_area": timezones,
                 "is_chinese_email": is_chinese_email,
                 "raw_location": raw_location,
                 "city": city,
                 "province": province,
                 "cctld": cctld,
                 "company": company,
                 "github_login": github_login})
            if len(bulk_data) == insert_size:
                self.insert_into_ck(
                    bulk_data, 'github_id_main_tz_map_v2')
        #
        if bulk_data:
            self.insert_into_ck(
                bulk_data, 'github_id_main_tz_map_v2')

    def insert_into_ck(self, bulk_data, table_name):
        if bulk_data:
            count = self.ck_client.execute(f'insert into table {table_name} values', bulk_data)
            logger.info(f'已插入数据{count}')
            bulk_data.clear()

    def code_owner_location(self, owner, repo):
        sql_ = """ """
        if owner == 'kubernetes' and repo == 'kubernetes':
            sql_ = """
            insert into table code_owners_location
            select search_key__owner                    as owner,
       search_key__repo                     as repo,
       'login'                              as data_type,
       toUnixTimestamp(now())               as insert_at,
       github_login                         as login_or_email,
       member_type,
       month,
       if(location == '', '其他', location) as inferred_location
from (select search_key__owner,
             search_key__repo,
             github_login,
             toYYYYMM(authored_date)               as month,
             JSONExtractString(misc, 'owner_type') as member_type
      from (
               select *
               from code_owners
--                where search_key__repo = 'team'
               where github_login != ''
                 and search_key__owner = 'kubernetes'
                 and search_key__repo = 'kubernetes'
                 and github_login global not in (select robot_login_email from robot_login_email)
               )
      group by search_key__owner, search_key__repo, github_login, month, member_type) as a global
         left join (select * from github_login_location) as b
                   on a.github_login = b.github_login
            """
            self.ck_client.execute_no_params(sql_)
            self.ck_client.execute_no_params(
                f"optimize table code_owners_location_local on cluster replicated partition '{owner}'")

    def code_owner_company(self, owner, repo):
        if owner == 'kubernetes' and repo == 'kubernetes':
            sql_ = """
            insert into table code_owners_company
            select search_key__owner      as owner,
       search_key__repo       as repo,
       'login'                as data_type,
       toUnixTimestamp(now()) as insert_at,
       github_login           as login_or_email,
       member_type,
       month,
       if(company='','unknow',company)                as inferred_company
from (select search_key__owner,
             search_key__repo,
             github_login,
             toYYYYMM(authored_date)               as month,
             JSONExtractString(misc, 'owner_type') as member_type
      from (
               select *
               from code_owners
--                where search_key__repo = 'team'
               where github_login != ''
                 and search_key__owner = 'kubernetes'
                 and search_key__repo = 'kubernetes'
                 and github_login global not in (select robot_login_email from robot_login_email)
               )
      group by search_key__owner, search_key__repo, github_login, month, member_type) as a global
         left join (select github_login,
                           company,
                           if(start_at < 9999, toInt64(concat(toString(start_at), '01')), start_at) as start_at,
                           if(end_at < 9999, toInt64(concat(toString(end_at), '01')), end_at)       as end_at
                    from github_login_company_time_range_v1) as b on
    a.github_login = b.github_login
where (month >= start_at
    and month <= end_at)
   or company = ''
            """
            self.ck_client.execute_no_params(sql_)
            self.ck_client.execute_no_params(
                f"optimize table code_owners_company_local on cluster replicated partition '{owner}'")






"""
建表使用
create table default.email_company_time_range_local on cluster replicated
(
    inserted_at  Int64,
    email String,
    company      String,
    start_at     Int64,
    end_at       Int64
)
    engine = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/email_company_time_range_local',
             '{replica}',
             inserted_at)
        ORDER BY (email, company, start_at, end_at)
        SETTINGS index_granularity = 8192;


create table default.email_company_time_range on cluster replicated as email_company_time_range_local
    engine = Distributed('replicated', 'default', 'email_company_time_range_local', halfMD5(email));



create table default.github_login_company_time_range_v1_local on cluster replicated
(
    inserted_at Int64,
    github_login String,
    company       String,
    start_at      Int64,
    end_at    Int64
)

    engine = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/github_login_company_time_range_v1_local', '{replica}',
             inserted_at)
        ORDER BY (github_login, company, start_at, end_at)
        SETTINGS index_granularity = 8192;


create table default.github_login_company_time_range_v1 on cluster replicated as github_login_company_time_range_v1_local
    engine = Distributed('replicated', 'default', 'github_login_company_time_range_v1_local', halfMD5(github_login));
    
    create table default.github_login_location_local on cluster replicated
(
    inserted_at  Int64,
    github_login String,
    location     String
)
    engine = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/github_login_location_local', '{replica}',
             inserted_at)
        ORDER BY (github_login)
        SETTINGS index_granularity = 8192;
        
        
        

create table default.github_login_location on cluster replicated as github_login_location_local
    engine = Distributed('replicated', 'default', 'github_login_location_local', halfMD5(github_login));




create table default.github_email_location_local on cluster replicated
(
    inserted_at              Int64,
    email                    String,
    location                 String,
    is_chinese_email_address String
)
    engine = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/github_email_location_local', '{replica}',
             inserted_at)
        ORDER BY (email, location, is_chinese_email_address)
        SETTINGS index_granularity = 8192;



create table default.github_email_location on cluster replicated as github_email_location_local
    engine = Distributed('replicated', 'default', 'github_email_location_local', halfMD5(email));
"""
