from . import init_profile_commen
import itertools
from loguru import logger


def load_github_profile(github_tokens, opensearch_conn_infos, github_users_logins):
    """Get GitHub profiles by logins"""
    # 处理login list为set;
    github_users_logins = list(set(github_users_logins))
    # 查询os
    # os有不作处理
    # os没有则查询github添加os
    github_tokens_iter = itertools.cycle(github_tokens)
    init_profile_commen.put_profile_into_opensearch(github_users_logins, github_tokens_iter, opensearch_conn_infos)
    logger.info(load_github_profile.__doc__)
