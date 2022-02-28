import datetime
from loguru import logger
from oss_know.libs.util.clickhouse_driver import CKServer
from oss_know.libs.util.base import infer_country_from_emailcctld, infer_country_from_emaildomain, \
    infer_company_from_emaildomain, infer_country_from_company
from oss_know.libs.base_dict.clickhouse import CLICKHOUSE_EMAIL_ADDRESS, EMAIL_ADDRESS_SEARCH_KEY__UPDATED_AT, \
    EMAIL_ADDRESS_SEARCH_KEY__EMAIL, EMAIL_ADDRESS_EMIAL, EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_EMAILCCTLD, \
    EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_EMAILDOMAIN, EMAIL_ADDRESS_COMPANY_INFERRED_FROM_EMAIL, \
    EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_COMPANY


def load_all_email_address(clickhouse_server_info):
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])

    gits_sql = "SELECT DISTINCT author_email,committer_email FROM gits"
    gits_email = ck.execute_no_params(gits_sql)
    all_email_address_dict = {}
    for author_email, committer_email in gits_email:
        for item in author_email, committer_email:
            if item:
                all_email_address_dict[item] = 0
    for k, v in all_email_address_dict.items():
        committer_id_sql = f"SELECT DISTINCT committer__id,commit__committer__email from github_commits WHERE commit__committer__email = '{k}'"
        committer_id = ck.execute_no_params(committer_id_sql)
        author_id_sql = f"SELECT DISTINCT author__id,commit__author__email from github_commits WHERE commit__author__email = '{k}'"
        author_id = ck.execute_no_params(author_id_sql)
        for item in committer_id, author_id:
            if item and item[0][0]:
                email = item[0][1]
                id = item[0][0]
                all_email_address_dict[email] = id

    profile_sql = "SELECT DISTINCT email,id FROM github_profile"
    profile_email_id_pair = ck.execute_no_params(profile_sql)
    for email, id in profile_email_id_pair:
        if email:
            all_email_address_dict[email] = id

    get_profile_table_columns_sql = "select distinct name from system.columns where database = 'default' AND table = 'github_profile'"
    profile_columns = ck.execute_no_params(get_profile_table_columns_sql)
    profile_columns_len = len(profile_columns)
    profile_value = {}

    values_to_insert = []
    total_count = len(all_email_address_dict)
    page_count = 5000
    total_page = total_count // page_count
    if total_count % page_count:
        total_page = total_page + 1
    count = 1
    page_num = 1
    for k, v in all_email_address_dict.items():
        value = {}
        value[EMAIL_ADDRESS_SEARCH_KEY__UPDATED_AT] = int(datetime.datetime.now().timestamp() * 1000)
        value[EMAIL_ADDRESS_SEARCH_KEY__EMAIL] = k
        value[EMAIL_ADDRESS_EMIAL] = k
        country_by_emailcctld = infer_country_from_emailcctld(k)
        country_by_emaildomain = infer_country_from_emaildomain(k)
        company_by_email = infer_company_from_emaildomain(k)
        for m, n in (EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_EMAILCCTLD, country_by_emailcctld), (
                EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_EMAILDOMAIN, country_by_emaildomain), (
                            EMAIL_ADDRESS_COMPANY_INFERRED_FROM_EMAIL, company_by_email):
            if n:
                value[m] = n
            else:
                value[m] = ''
        if company_by_email:
            country_by_company = infer_country_from_company(company_by_email)
            if not country_by_company:
                value[EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_COMPANY] = country_by_company
        else:
            value[EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_COMPANY] = ''

        github_profile_sql = f"select * from github_profile where search_key__updated_at = (select MAX(search_key__updated_at) from github_profile where github_profile.id = '{v}'); "
        github_profile_list = ck.execute_no_params(github_profile_sql)
        if github_profile_list:
            github_profile = github_profile_list[0]
            for index in range(1, profile_columns_len):
                profile_property = github_profile[index]
                if isinstance(profile_property, str):
                    profile_property = profile_property.replace('\'', '\"')
                profile_value['github__profile__' + profile_columns[index][0]] = profile_property
            value = dict(value, **profile_value)
            values_to_insert.append(value)
            if (page_num < total_page and count < page_count) or (
                    page_num == total_page and count < total_count - (page_num - 1) * page_count):
                count = count + 1
            else:
                page_num = page_num + 1
                count = 1
                insert_email_address_sql = f"INSERT INTO {CLICKHOUSE_EMAIL_ADDRESS} (*) VALUES"
                ck.execute(insert_email_address_sql, values_to_insert)
                values_to_insert = []
    if values_to_insert:
        insert_email_address_sql = f"INSERT INTO {CLICKHOUSE_EMAIL_ADDRESS} (*) VALUES"
        ck.execute(insert_email_address_sql, values_to_insert)
    ck.close()