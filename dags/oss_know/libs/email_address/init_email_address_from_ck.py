import datetime

from oss_know.libs.util.clickhouse_driver import CKServer
from oss_know.libs.util.base import infer_country_from_emailcctld, infer_country_from_emaildomain, \
    infer_company_from_emaildomain, infer_country_from_company


def load_all_email_address(clickhouse_server_info):
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])

    gits_sql = "SELECT DISTINCT author_email,committer_email FROM gits"
    gits_email_from_ck = ck.execute_no_params(gits_sql)
    all_email_address_dict = {}
    for author_email, committer_email in gits_email_from_ck:
        for item in author_email, committer_email:
            if item:
                all_email_address_dict[item] = 0
    if all_email_address_dict:
        for k, v in all_email_address_dict.items():
            github_committer_id_from_committer_email_sql = f"SELECT DISTINCT author__id,commit__author__email from github_commits WHERE commit__author__email = '{k}'"
            github_committer_id_from_committer_email = ck.execute_no_params(
                github_committer_id_from_committer_email_sql)
            github_author_id_from_author_email_sql = f"SELECT DISTINCT committer__id,commit__committer__email from github_commits WHERE commit__committer__email = '{k}'"
            github_author_id_from_author_email = ck.execute_no_params(github_author_id_from_author_email_sql)
            for item in github_committer_id_from_committer_email, github_author_id_from_author_email:
                if item and item[0][0]:
                    all_email_address_dict[item[0][1]] = item[0][0]

    github_profile_sql = "SELECT DISTINCT email,id FROM github_profile"
    github_profile_email_from_ck = ck.execute_no_params(github_profile_sql)
    for email, id in github_profile_email_from_ck:
        if email:
            all_email_address_dict[email] = id

    # count = 0
    insert_email_address_data_params = []
    if all_email_address_dict:
        for k, v in all_email_address_dict.items():
            insert_email_address_data = []
            insert_email_address_data.append(int(datetime.datetime.now().timestamp() * 1000))
            insert_email_address_data.extend([k, k])
            country_inferred_from_emailcctld = infer_country_from_emailcctld(k)
            country_inferred_from_emaildomain = infer_country_from_emaildomain(k)
            company_inferred_from_email = infer_company_from_emaildomain(k)
            for item in country_inferred_from_emailcctld, country_inferred_from_emaildomain, company_inferred_from_email:
                if not item:
                    item = ''
                insert_email_address_data.append(item)
            if company_inferred_from_email:
                country_inferred_from_company = infer_country_from_company(company_inferred_from_email)
                if not country_inferred_from_company:
                    country_inferred_from_company = ''
                insert_email_address_data.append(country_inferred_from_company)
            else:
                insert_email_address_data.append('')
            github_profile_by_id_sql = f"select * from github_profile where search_key__updated_at = (select MAX(search_key__updated_at) from github_profile where github_profile.id = '{v}'); "
            github_profile_by_id = ck.execute_no_params(github_profile_by_id_sql)
            if github_profile_by_id:
                # count = count + 1
                # if count == 10:
                #     break
                tuple_length = len(github_profile_by_id[0])
                for index in range(1, tuple_length):
                    github_profile_by_id_item = github_profile_by_id[0][index]
                    if isinstance(github_profile_by_id_item, str):
                        github_profile_by_id_item = github_profile_by_id_item.replace('\'', '\"')
                    insert_email_address_data.append(github_profile_by_id_item)
                insert_email_address_data_params.append(tuple(insert_email_address_data))
            insert_email_address_sql = "INSERT INTO email_address_test03_easy (*) VALUES"
            ck.execute(insert_email_address_sql, insert_email_address_data_params)

    # todo: 从clickhouse中的gits中根据指定email获取该email参与过的owner、repo
    # for email_address in all_email_address:
    #     gits_owner_repo_sql = f"SELECT DISTINCT search_key__owner,search_key__repo FROM gits WHERE author_email = '{email_address}' OR committer_email = '{email_address}' "
    #     gits_owner_repo = ck.execute_no_params(gits_owner_repo_sql)
    #     print(type(gits_owner_repo))
    #     print(gits_owner_repo)

    # todo: 从clickhouse中的github_commit中根据指定email获取该email对应的github profile对应的id
    # github_id_from_github_commits = set()
    # for email_address in all_email_address:
    #     github_committer_id_from_committer_email_sql = f"SELECT DISTINCT author__id from github_commits WHERE commit__author__email = '{email_address}'"
    #     github_author_id_from_author_email_sql = f"SELECT DISTINCT committer__id from github_commits WHERE commit__committer__email = '{email_address}'"
    #     github_committer_id_from_committer_email = ck.execute_no_params(github_committer_id_from_committer_email_sql)
    #     github_author_id_from_author_email = ck.execute_no_params(github_author_id_from_author_email_sql)
    #     for item in github_committer_id_from_committer_email, github_author_id_from_author_email:
    #         if item and item[0][0]:
    #             github_id_from_github_commits.add(item[0][0])
    # print(github_id_from_githgithub_profile_sqlub_commits)

    # todo: 根据github profile对应的id查找在github issues中的owner、repo信息
    # profile_id = 200109
    # owner_repo_by_profile_id_from_github_issues_sql = f"SELECT DISTINCT search_key__owner,search_key__repo,has(assignees.id,{profile_id}) as has_id from github_issues where user__id={profile_id} OR assignee__id={profile_id} or milestone__creator__id={profile_id} or has_id=1 "
    # owner_repo_by_profile_id_from_github_issues = ck.execute_no_params(owner_repo_by_profile_id_from_github_issues_sql)
    # if owner_repo_by_profile_id_from_github_issues:
    #     for owner_repo in owner_repo_by_profile_id_from_github_issues:
    #         print(owner_repo[0])
    #         print(owner_repo[1])
    #         print(owner_repo[2])
    # print(owner_repo_by_profile_id_from_github_issues)

    # todo: 从clickhouse中获取的email对接晨琪的根据email推断信息
    # for email_address in all_email_address:
    #     if email_address:
    #         country_inferred_from_emailcctld = infer_country_from_emailcctld(email_address)
    #         country_inferred_from_emaildomain = infer_country_from_emaildomain(email_address)
    #         company_inferred_from_email = infer_company_from_emaildomain(email_address)
    #         for item in country_inferred_from_emailcctld, country_inferred_from_emaildomain, company_inferred_from_email:
    #             if item:
    #                 print(item)
    #         if company_inferred_from_email:
    #             country_inferred_from_company = infer_country_from_company(company_inferred_from_email)
    #             if country_inferred_from_company:
    #                 print(country_inferred_from_company)

    # github_profile_sql = "select * from github_profile where id = 3309585"
    # github_profile_email_from_ck = ck.execute_no_params(github_profile_sql)
    # print(type(github_profile_email_from_ck))
    # print(github_profile_email_from_ck)

    ck.close()
