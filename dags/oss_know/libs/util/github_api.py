import copy

from ..util.base import do_get_result
from ..util.log import logger


class GithubAPIException(Exception):
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status


class GithubAPI:
    github_headers = {'Connection': 'keep-alive', 'Accept-Encoding': 'gzip, deflate, br', 'Accept': '*/*',
                      'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36', }

    def get_github_commits(self, http_session, github_tokens_iter, owner, repo, page, since, until):
        if not http_session or not github_tokens_iter or not owner or not repo or not page or not since or not until:
            raise GithubAPIException(f"缺少必须的参数")

        url = "https://api.github.com/repos/{owner}/{repo}/commits".format(
            owner=owner, repo=repo)
        headers = copy.deepcopy(self.github_headers)
        headers.update({'Authorization': 'token %s' % next(github_tokens_iter)})
        params = {'per_page': 100, 'page': page, 'since': since, 'until': until}

        return do_get_result(http_session, url, headers, params)

    def get_github_issues(self, http_session, github_tokens_iter, owner, repo, page, since):
        url = "https://api.github.com/repos/{owner}/{repo}/issues".format(
            owner=owner, repo=repo)
        headers = copy.deepcopy(self.github_headers)
        headers.update({'Authorization': 'token %s' % next(github_tokens_iter)})
        params = {'state': 'all', 'per_page': 100, 'page': page, 'since': since}
        res = do_get_result(http_session, url, headers, params)

        logger.info(f"url:{url}, \n headers:{headers}, \n params：{params}")

        return res

    def get_github_profiles(self, http_session, github_tokens_iter, login_info):
        """Get GitHub user's latest profile from GitHUb."""
        url = "https://api.github.com/users/{login_info}".format(
            login_info=login_info)
        headers = copy.deepcopy(self.github_headers)
        headers.update({'Authorization': 'token %s' % next(github_tokens_iter)})
        params = {}
        now_github_profile = {}
        try:
            req = do_get_result(http_session, url, headers, params)
            now_github_profile = req.json()
        except TypeError as e:
            print("捕获airflow抛出的TypeError:", e)
            return {'login': login_info, 'status': 'User status is abnormal.', 'company': None, 'location': None,
                    'email': None}
        logger.info(f"Get GitHub {login_info}'s latest profile from GitHUb.")
        return now_github_profile

    def get_github_issues_timeline(self, http_session, github_tokens_iter, owner, repo, number, page):
        url = "https://api.github.com/repos/{owner}/{repo}/issues/{number}/timeline".format(
            owner=owner, repo=repo, number=number)
        headers = copy.deepcopy(self.github_headers)
        headers.update({'Authorization': 'token %s' % next(github_tokens_iter)})
        params = {'per_page': 100, 'page': page}
        res = do_get_result(http_session, url, headers, params)
        return res

    def get_github_issues_comments(self, http_session, github_tokens_iter, owner, repo, number, page):
        url = "https://api.github.com/repos/{owner}/{repo}/issues/{number}/comments".format(
            owner=owner, repo=repo, number=number)
        headers = copy.deepcopy(self.github_headers)
        headers.update({'Authorization': 'token %s' % next(github_tokens_iter)})
        params = {'per_page': 100, 'page': page}
        res = do_get_result(http_session, url, headers, params)
        return res

    def get_github_pull_requests(self, http_session, github_tokens_iter, owner, repo, page, since):
        url = "https://api.github.com/repos/{owner}/{repo}/pulls".format(
            owner=owner, repo=repo)
        headers = copy.deepcopy(self.github_headers)
        headers.update({'Authorization': 'token %s' % next(github_tokens_iter)})
        params = {'state': 'all', 'per_page': 100, 'page': page, 'since': since}
        res = do_get_result(http_session, url, headers, params)
        return res