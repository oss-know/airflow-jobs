import copy

from oss_know.libs.util.base import do_get_github_result
from oss_know.libs.exceptions import GithubNonExistingUserError
from oss_know.libs.util.log import logger



class GithubAPI:
    github_headers = {'Connection': 'keep-alive', 'Accept-Encoding': 'gzip, deflate, br', 'Accept': '*/*',
                      'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36', }

    def get_github_commits(self, http_session, token_proxy_accommodator, owner, repo, page, since, until):
        url = f"https://api.github.com/repos/{owner}/{repo}/commits"
        headers = copy.deepcopy(self.github_headers)
        params = {'per_page': 100, 'page': page, 'since': since, 'until': until}

        return do_get_github_result(http_session, url, headers, params, token_proxy_accommodator)

    def get_github_issues(self, http_session, token_proxy_accommodator, owner, repo, page, since):
        url = f"https://api.github.com/repos/{owner}/{repo}/issues"
        headers = copy.deepcopy(self.github_headers)
        params = {'state': 'all', 'per_page': 100, 'page': page, 'since': since}
        res = do_get_github_result(http_session, url, headers, params, accommodator=token_proxy_accommodator)

        logger.info(f"url:{url}, \n headers:{headers}, \n params：{params}")

        return res

    def get_latest_github_profile(self, http_session, token_proxy_accommodator, user_id):
        """Get GitHub user's latest profile from GitHUb."""
        url = f"https://api.github.com/user/{user_id}"
        headers = copy.deepcopy(self.github_headers)
        params = {}
        try:
            req = do_get_github_result(http_session, url, headers, params, accommodator=token_proxy_accommodator)
            latest_github_profile = req.json()
        except (TypeError, GithubNonExistingUserError) as e:
            logger.error(f"Failed to get github profile: {e}, return an empty one")
            return {'login': '', 'id': user_id, 'node_id': '', 'avatar_url': '', 'gravatar_id': '', 'url': '',
                    'html_url': '',
                    'followers_url': '', 'following_url': '', 'gists_url': '', 'starred_url': '',
                    'subscriptions_url': '',
                    'organizations_url': '', 'repos_url': '', 'events_url': '', 'received_events_url': '', 'type': '',
                    'site_admin': False, 'name': '', 'company': '', 'blog': '', 'location': '', 'email': '',
                    'hireable': False,
                    'bio': '', 'twitter_username': '', 'public_repos': 0, 'public_gists': 0, 'followers': 0,
                    'following': 0,
                    'created_at': '1970-01-01T00:00:00Z', 'updated_at': '1970-01-01T00:00:00Z',
                    'country_inferred_from_email_cctld': '', 'country_inferred_from_email_domain_company': '',
                    'country_inferred_from_location': '', 'country_inferred_from_company': '',
                    'final_company_inferred_from_company': '',
                    'company_inferred_from_email_domain_company': '', 'inferred_from_location': ''}
        else:
            logger.info(f"Get GitHub {user_id}'s latest profile from GitHUb.")
            return latest_github_profile

    def get_github_issues_timeline(self, http_session, token_proxy_accommodator, owner, repo, number, page):
        url = f"https://api.github.com/repos/{owner}/{repo}/issues/{number}/timeline"
        headers = copy.deepcopy(self.github_headers)
        params = {'per_page': 100, 'page': page}
        res = do_get_github_result(http_session, url, headers, params, token_proxy_accommodator)
        return res

    def get_github_issues_comments(self, http_session, token_proxy_accommodator, owner, repo, number, page):
        url = f"https://api.github.com/repos/{owner}/{repo}/issues/{number}/comments"
        headers = copy.deepcopy(self.github_headers)
        params = {'per_page': 100, 'page': page}
        res = do_get_github_result(http_session, url, headers, params, token_proxy_accommodator)
        return res

    def get_github_pull_requests(self, http_session, token_proxy_accommodator, owner, repo, page, since):
        url = "https://api.github.com/repos/{owner}/{repo}/pulls".format(
            owner=owner, repo=repo)
        headers = copy.deepcopy(self.github_headers)
        params = {'state': 'all', 'per_page': 100, 'page': page, 'since': since}
        res = do_get_github_result(http_session, url, headers, params, token_proxy_accommodator)
        return res
