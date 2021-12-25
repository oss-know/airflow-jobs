from . import init_profile_commen


def load_github_profile(**init_profile_dict):

    opensearch_client = init_profile_dict['opensearch_client']
    all_commits_users_set = init_profile_dict['logins']
    OPEN_SEARCH_GITHUB_PROFILE_INDEX = init_profile_dict['OPEN_SEARCH_GITHUB_PROFILE_INDEX']
    github_tokens_iter = init_profile_dict['github_tokens_iter']
    opensearch_conn_infos = init_profile_dict['opensearch_conn_infos']
    init_profile_commen.put_profile_into_opensearch(opensearch_client, all_commits_users_set,
                                                    OPEN_SEARCH_GITHUB_PROFILE_INDEX, github_tokens_iter,
                                                    opensearch_conn_infos)
    return "End::load_github_profile"


def load_github_profile_no_kwargs(opensearch_client, logins, github_tokens_iter, opensearch_conn_infos):
    opensearch_client = opensearch_client
    all_commits_users_set = logins
    OPEN_SEARCH_GITHUB_PROFILE_INDEX = OPEN_SEARCH_GITHUB_PROFILE_INDEX
    github_tokens_iter = github_tokens_iter
    opensearch_conn_infos = opensearch_conn_infos
    init_profile_commen.put_profile_into_opensearch(opensearch_client, all_commits_users_set,
                                                    OPEN_SEARCH_GITHUB_PROFILE_INDEX, github_tokens_iter,
                                                    opensearch_conn_infos)