import copy
import datetime
from grimoire_elk.enriched.mbox import MBoxEnrich
from loguru import logger
from oss_know.libs.util.base import infer_country_from_emailcctld, infer_country_from_emaildomain, \
    infer_company_from_emaildomain, infer_country_from_company, get_clickhouse_client
from oss_know.libs.base_dict.clickhouse import CLICKHOUSE_EMAIL_ADDRESS, EMAIL_ADDRESS_SEARCH_KEY__UPDATED_AT, \
    EMAIL_ADDRESS_SEARCH_KEY__EMAIL, EMAIL_ADDRESS_EMIAL, EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_EMAILCCTLD, \
    EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_EMAILDOMAIN, EMAIL_ADDRESS_COMPANY_INFERRED_FROM_EMAIL, \
    EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_COMPANY

from oss_know.libs.email_address.email_address_default_tplt import EMAIL_ADDRESS_DEFAULT_TPLT


def load_all_email_address(clickhouse_server_info):
    ck = get_clickhouse_client(clickhouse_server_info)
    init_email_address_dict = {}
    update_email_address = set()
    mbox_helper = MBoxEnrich()
    maillists_enriched_sql = "select DISTINCT From,To from maillists_enriched"
    email_tuples = ck.execute_no_params(maillists_enriched_sql)
    for email_tuple in email_tuples:
        for origin_email_str in email_tuple:
            if origin_email_str:
                origin_email_list = origin_email_str.split(',')
                for origin_email in origin_email_list:
                    identity = mbox_helper.get_sh_identity(origin_email)
                    email = identity["email"]
                    if email and '@' in email:
                        init_email_address_dict[email] = 0

    gits_sql = "SELECT DISTINCT author_email,committer_email FROM gits"
    gits_email = ck.execute_no_params(gits_sql)
    for author_email, committer_email in gits_email:
        for item in author_email, committer_email:
            if email_is_existed(item, ck):
                update_email_address.add(item)
            elif item:
                init_email_address_dict[item] = 0

    for k, v in init_email_address_dict.items():
        committer_id_sql = f"SELECT DISTINCT committer__id,commit__committer__email from github_commits WHERE commit__committer__email = '{k}'"
        committer_id = ck.execute_no_params(committer_id_sql)
        author_id_sql = f"SELECT DISTINCT author__id,commit__author__email from github_commits WHERE commit__author__email = '{k}'"
        author_id = ck.execute_no_params(author_id_sql)
        for item in committer_id, author_id:
            if item and item[0][0]:
                email = item[0][1]
                id = item[0][0]
                init_email_address_dict[email] = id

    profile_sql = "SELECT DISTINCT email,id FROM github_profile"
    profile_email_id_pair = ck.execute_no_params(profile_sql)
    for email, id in profile_email_id_pair:
        if email_is_existed(email, ck):
            update_email_address.add(email)
        else:
            init_email_address_dict[email] = id

    need_update_email_id_pair = {}
    for email in update_email_address:
        profile_updated_sql = f"select updated_at from github_profile where search_key__updated_at = (select MAX(search_key__updated_at) from github_profile where github_profile.email = '{email}'); "
        profile_updated = ck.execute_no_params(profile_updated_sql)
        email_profile_updated_sql = f"SELECT github__profile__updated_at FROM {CLICKHOUSE_EMAIL_ADDRESS}  WHERE email = '{email}'"
        email_profile_updated = ck.execute_no_params(email_profile_updated_sql)
        if profile_updated != email_profile_updated:
            profile_id_sql = f"select id from github_profile where search_key__updated_at = (select MAX(search_key__updated_at) from github_profile where github_profile.email = '{email}'); "
            profile_id = ck.execute_no_params(profile_id_sql)
            if profile_id:
                need_update_email_id_pair[email] = profile_id

            ck_cluster_name = clickhouse_server_info["CLUSTER_NAME"]
            delete_need_update_sql = f"ALTER TABLE {CLICKHOUSE_EMAIL_ADDRESS} on cluster {ck_cluster_name} delete where email = '{email}'"
            ck.execute_no_params(delete_need_update_sql)

    init_email_address_dict = dict(init_email_address_dict, **need_update_email_id_pair)
    values_to_insert = []
    total_count = len(init_email_address_dict)
    page_count = 5000
    total_page = total_count // page_count
    if total_count % page_count:
        total_page = total_page + 1
    count = 1
    page_num = 1
    for k, v in init_email_address_dict.items():
        value = copy.deepcopy(EMAIL_ADDRESS_DEFAULT_TPLT)
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
            profile_value = get_profile_value(profile_id=v, clickhouse_client=ck)
            value = dict(value, **profile_value)
            values_to_insert.append(value)
            if (page_num < total_page and count < page_count) or (
                    page_num == total_page and count < total_count - (page_num - 1) * page_count):
                count = count + 1
            else:
                page_num = page_num + 1
                count = 1
                insert_email_address_sql = f"INSERT INTO {CLICKHOUSE_EMAIL_ADDRESS} (*) VALUES"
                logger.debug(f"------------values_to_insert{values_to_insert}")
                ck.execute(insert_email_address_sql, values_to_insert)
                values_to_insert = []
    if values_to_insert:
        insert_email_address_sql = f"INSERT INTO {CLICKHOUSE_EMAIL_ADDRESS} (*) VALUES"
        ck.execute(insert_email_address_sql, values_to_insert)
    ck.close()


def email_is_existed(email, ck):
    is_existed = False
    if not email:
        return is_existed
    sql = f"select * from {CLICKHOUSE_EMAIL_ADDRESS} where search_key__email ='{email}'"
    email_domain = ck.execute_no_params(sql)
    if email_domain:
        is_existed = True
    return is_existed


def get_profile_value(profile_id, clickhouse_client):
    get_profile_table_columns_sql = "select distinct name from system.columns where database = 'default' AND table = 'github_profile'"
    profile_columns = clickhouse_client.execute_no_params(get_profile_table_columns_sql)
    profile_columns_len = len(profile_columns)
    profile_value = {}
    github_profile_sql = f"select * from github_profile where search_key__updated_at = (select MAX(search_key__updated_at) from github_profile where github_profile.id = '{profile_id}'); "
    github_profile_list = clickhouse_client.execute_no_params(github_profile_sql)
    if github_profile_list:
        github_profile = github_profile_list[0]
        for index in range(1, profile_columns_len):
            profile_property = github_profile[index]
            if isinstance(profile_property, str):
                profile_property = profile_property.replace('\'', '\"')
            profile_value['github__profile__' + profile_columns[index][0]] = profile_property
    return profile_value
    ck.close()
