import json
import re
from concurrent.futures import ThreadPoolExecutor
from os import path, makedirs
from oss_know.libs.util.log import logger
import git

from oss_know.libs.util.clickhouse_driver import CKServer


class CodeOwnerWatcher:
    TARGET_FILES = {}
    BATCH_SIZE = 5000
    OWNER = ''
    REPO = ''
    ORIGIN = ''

    def __init__(self, local_repo_dir, ck_conn_info):
        self.local_repo_path = f'{local_repo_dir}/{self.__class__.OWNER}/{self.__class__.REPO}'
        logger.info(f'Init repo {self.__class__.OWNER}/{self.__class__.REPO} to {self.local_repo_path}')
        self.git_repo = None
        self.envolved_commits_map = {}
        self.envolved_commits = []
        self.ck_client = CKServer(
            host=ck_conn_info['HOST'],
            port=ck_conn_info['PORT'],
            user=ck_conn_info['USER'],
            password=ck_conn_info['PASSWD'],
            database=ck_conn_info['DATABASE'],
        )

    def collect_file_rev_list(self, target_file):
        if not self.git_repo:
            raise ValueError('git repo not initialized')

        shas = self.git_repo.git.rev_list('HEAD', target_file).split()
        for sha in shas:
            if sha not in self.envolved_commits_map:
                self.envolved_commits_map[sha] = True

    def collect_local_commits(self):
        """
        Collect all envolved commits once from the local git repo
        """
        with ThreadPoolExecutor(max_workers=10) as worker:
            futures = []
            for target_file_path in self.__class__.TARGET_FILES.keys():
                futures.append(worker.submit(self.collect_file_rev_list, target_file_path))

            [f.result() for f in futures]

        self.envolved_commits = list(self.envolved_commits_map.keys())

    def init(self):
        repo_name = f'{self.__class__.OWNER}/{self.__class__.REPO}'
        if path.exists(self.local_repo_path):
            logger.info(f'Repo {repo_name} exists, pull origin')
            self.git_repo = git.Repo(self.local_repo_path)
            self.git_repo.remotes.origin.pull()
        else:
            logger.info(f'Repo {repo_name} does not exist, clone from remote')
            makedirs(self.local_repo_path, exist_ok=True)
            self.git_repo = git.Repo.clone_from(self.__class__.ORIGIN, self.local_repo_path)
        logger.info(f'Repo {repo_name} updated to the latest commit')

    def iter_history(self):
        for commit in self.envolved_commits:
            self.take_snapshot(commit)

    def take_snapshot(self, sha):
        commit = self.git_repo.commit(sha)

        for filepath in self.__class__.TARGET_FILES:
            try:
                target_file = commit.tree / filepath
            except KeyError as e:
                logger.info(f'Failed to analyze {filepath} in {sha}, error:{e}')
                continue

            file_content = target_file.data_stream.read().decode('utf-8')
            developer_info = self.get_developer_info(file_content)
            self.save_snapshot(sha, commit.authored_datetime, filepath, developer_info)

    def get_current_commits(self):
        sql = f'''
        select distinct hexsha from code_owners
        where search_key__owner='{self.__class__.OWNER}' and search_key__repo='{self.__class__.REPO}'
        '''
        result = self.ck_client.execute_no_params(sql)
        current_commits = [item[0] for item in result]
        return current_commits

    def watch(self):
        # Currently all key projects are hosted on GitHub. So the target file's latest history can be
        # analyzed from GitHub web page.
        # TODO Consider: since the repo is already cloned to local storage, is it necessary to parse
        #  github web page? Just pull and get rev list, then compare the list with local database, which
        #  seems to work perfectly.

        # TODO Compare the latest commits from github web page and local database
        # for file in self.__class__.TARGET_FILES:
        #     history_page_url = f'${self.repo_url}/commits/main/{file}'
        #     res = requests.get(history_page_url)
        #     soup = BeautifulSoup(res.content, 'html.parser')
        #     elements = soup.find_all('a', {'class': "Button--secondary Button--small Button text-mono f6"})
        #     for element in elements:
        #         href = element['href']
        #         commit = href.split('/')[-1]
        #         print(commit)
        #         # TODO Assign the diff commits to self.envolved_commits
        #         self.iter_history()

        # By the higher level design, watch() is called after init(), which makes sure the git repo is
        # updated(with pull or a fresh clone)
        current_commits = self.get_current_commits()
        self.collect_local_commits()  # related local commits will be saved to self.envolved_commit
        if current_commits:
            diff = set(self.envolved_commits).difference(set(current_commits))
            self.envolved_commits = diff
            logger.info(f'New code owner info from commits: {diff}')

        self.iter_history()

    def save_snapshot(self, sha, authored_date, filepath, developer_info):
        # Implemented in concrete classes
        rows = []
        for d in developer_info:
            row = {
                'search_key__owner': self.__class__.OWNER,
                'search_key__repo': self.__class__.REPO,
                'search_key__origin': self.__class__.ORIGIN,
                'hexsha': sha,
                'authored_date': authored_date,
                'filepath': filepath,
            }
            self.apply_row_info(row, filepath, d)

            rows.append(row)
            if len(rows) >= self.__class__.BATCH_SIZE:
                self.ck_client.execute('insert into table code_owners values', rows)

        if rows:
            self.ck_client.execute('insert into table code_owners values', rows)

    def get_developer_info(self, file_path):
        pass

    def apply_row_info(self, row, filepath, developer):
        pass


class LLVMCodeOwnerWatcher(CodeOwnerWatcher):
    TARGET_FILES = {
        'libcxx/CREDITS.TXT': True,
        'libcxxabi/CREDITS.TXT': True,
        'compiler-rt/CREDITS.TXT': True,
        'llvm/CREDITS.TXT': True,
        'pstl/CREDITS.txt': True,
        'libclc/CREDITS.TXT': True,
        'openmp/CREDITS.txt': True,
        'polly/CREDITS.txt': True,
        'clang-tools-extra/CODE_OWNERS.TXT': True,
        'compiler-rt/CODE_OWNERS.TXT': True,
        'llvm/CODE_OWNERS.TXT': True,
        'flang/CODE_OWNERS.TXT': True,
        'bolt/CODE_OWNERS.TXT': True,
        # 'lldb/CODE_OWNERS.txt': True, # changed to a new rst file 2 days ago, need a specific analyzer
        'lld/CODE_OWNERS.TXT': True,
    }

    OWNER = 'llvm'
    REPO = 'llvm-project'
    ORIGIN = 'https://github.com/llvm/llvm-project'

    KEY_MAP = {
        'N': 'name',
        'H': 'phabricator_handle',
        'E': 'email',
        'D': 'description'
    }

    def get_developer_info(self, content):
        current_module = []
        current_developer = None
        for raw_line in content.split('\n'):
            line = raw_line.strip()
            if re.search('[NEHD]:\ ', line):
                if line.startswith('N:'):
                    # Append the previously stored developer info
                    # and move to the next one
                    if current_developer:
                        current_module.append(current_developer)

                    name = line.split(': ')[-1]
                    current_developer = {
                        "name": name
                    }
                else:
                    parts = line.split(': ')
                    key = LLVMCodeOwnerWatcher.KEY_MAP[parts[0]]
                    val = parts[1]
                    current_developer[key] = val

        # Don't forget the last developer
        if current_developer:
            current_module.append(current_developer)

        return current_module

    def apply_row_info(self, row, filepath, developer):
        row['module'] = filepath
        row['name'] = developer.get('name') or ''
        row['email'] = developer.get('email') or ''
        row['github_login'] = ''
        row['misc'] = json.dumps({'description': developer['description']}) if 'description' in developer else ''


class PytorchCodeOwnerWatcher(CodeOwnerWatcher):
    TARGET_FILES = {
        'CODEOWNERS': True,
    }
    OWNER = 'pytorch'
    REPO = 'pytorch'
    ORIGIN = 'https://github.com/pytorch/pytorch'

    @staticmethod
    def analyze_module_line(line):
        parts = line.split()
        module_path = parts[0]
        developers = list(map(lambda d: d.strip().replace('@', ''), parts[1:]))
        return module_path, developers

    def get_developer_info(self, code_owner_content):
        desc = ''
        analyzing_modules = False

        developer_info = []

        for line in code_owner_content.split('\n'):
            content = line.strip()

            if not content:
                desc = ''
                analyzing_modules = False
            else:
                if content.startswith('#'):
                    if analyzing_modules:
                        # Exit analyzing module state
                        desc = content + '\n'
                        analyzing_modules = False
                    else:
                        desc += content + '\n'
                else:
                    analyzing_modules = True
                    module_path, developers = PytorchCodeOwnerWatcher.analyze_module_line(content)
                    for developer_github_login in developers:
                        developer_info.append((desc, module_path, developer_github_login))

        return developer_info

    def apply_row_info(self, row, filepath, developer_tup):
        desc, module_path, github_login = developer_tup

        row['module'] = module_path
        row['name'] = ''
        row['email'] = ''
        row['github_login'] = github_login
        row['misc'] = json.dumps({'description': desc})
