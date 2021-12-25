from . import init_profiles_by_github_commits
from . import init_profile_by_github_issues
from . import init_profile_by_github_issues_comments
from . import init_profile_by_github_issues_timeline
from . import init_profile_by_pull_requests


def load_github_repo_github_user_login(github_tokens, opensearch_conn_infos, owner, repo):
    init_profile_dict = {}
    init_profile_dict = init_profiles_by_github_commits.load_github_profile(github_tokens, opensearch_conn_infos, owner,
                                                                            repo)
    init_profile_logins = init_profile_dict['logins']
    init_profile_logins = init_profile_logins | init_profile_by_github_issues.load_github_profile_issues(github_tokens,
                                                                                                         opensearch_conn_infos,
                                                                                                         owner, repo)
    init_profile_logins = init_profile_logins | init_profile_by_github_issues_comments.load_github_profile_issues_comments(
        github_tokens,
        opensearch_conn_infos, owner,
        repo)
    init_profile_logins = init_profile_logins | init_profile_by_github_issues_timeline.load_github_profile_issues_timeline(
        github_tokens,
        opensearch_conn_infos, owner,
        repo)
    init_profile_logins = init_profile_logins | init_profile_by_pull_requests.load_github_profile_pull_requests(
        github_tokens, opensearch_conn_infos, owner,
        repo)
    init_profile_dict['login'] = init_profile_logins
    return init_profile_dict
