import requests
import json
import itertools


def sync_github_commits(github_tokens, opensearch_conn_infos, owner, repo, since=None, until=None):
    github_tokens_iter = itertools.cycle(github_tokens)

    for page in range(1):
        url = "https://api.github.com/repos/{owner}/{repo}/commits".format(
            owner=owner, repo=repo)
        headers = {'Authorization': 'token %s' % next(github_tokens_iter)}
        params = {'per_page': 100, 'page': page, 'since': since, 'until': until}
        r = requests.get(url, headers=headers, params=params)

        print("url:", url)
        print("headers", headers)
        print("params", params)
        print(json.dumps(r.json(), indent=2))
        raise Exception('获取github commits 失败！')

        if r.status_code != 200:
            print("url")
            print(url)
            print("headers")
            print(headers)
            print("params")
            print(params)
            print(json.dumps(r.json(), indent=2))
            raise Exception('获取github commits 失败！')
        else:
            print(r.status_code, r.text)
            return None
