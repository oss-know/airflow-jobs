import datetime
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

import git  # GitPython
import lizard
import timeout_decorator

from oss_know.libs.base_dict.variable_key import REPO_CLONE_DIR
from oss_know.libs.metrics.influence_metrics import MetricRoutineCalculation
from oss_know.libs.util.log import logger


def clone_(owner, repo, dir):
    url = f"https://github.com/{owner}/{repo}.git"

    try:
        if os.path.exists(dir):
            git_repo = git.Repo(dir)
            o = git_repo.remotes.origin
            logger.info(f'Code base{owner}/{repo} already exists, pull from remote')
            o.pull()
        else:
            logger.info(f'Code base{owner}/{repo} does not exists, clone from remote')
            git.Repo.clone_from(url, dir)
    except git.exc.GitError as e:
        logger.error(f'Failed to fetch code {url}, error: {str(e)}')
        return


def remove_repos(dir):
    if os.path.exists(dir):
        shutil.rmtree(dir)


def extract_metrics(owner, repo_name, repo_dir):
    metric = {'owner': owner, 'repo': repo_name, 'created_at': datetime.datetime.now()}

    repo = git.Repo(repo_dir)
    sha = repo.head.commit.hexsha
    metric['sha'] = sha

    mp2 = lizard.analyze([repo_dir], exts=lizard.get_extensions([]))
    # schema = OutputScheme(ext=lizard.get_extensions([]))
    res = lizard.AllResult(mp2)

    metric['nloc'] = res.as_fileinfo().nloc
    metric['average_nloc'] = res.as_fileinfo().average_nloc
    metric['average_cyclomatic_complexity'] = res.as_fileinfo().average_cyclomatic_complexity

    metric['average_token_count'] = res.as_fileinfo().average_token_count

    return metric


# @timeout_decorator.timeout(5)
def _analysis_single_file(file_path):
    result = lizard.analyze_files([file_path])

    nloc, num_funcs, func_nloc, token_count, cc = 0, 0, 0, 0, 0

    for r in result:
        nloc += r.nloc
        num_funcs += len(r.function_list)
        func_nloc += sum(f.nloc for f in r.function_list)
        token_count += sum(f.token_count for f in r.function_list)
        cc += sum(f.cyclomatic_complexity for f in r.function_list)

    return nloc, num_funcs, func_nloc, token_count, cc


def extract_metrics_safe(owner, repo_name, repo_dir):
    metric = {'owner': owner, 'repo': repo_name, 'created_at': datetime.datetime.now()}

    repo = git.Repo(repo_dir)
    sha = repo.head.commit.hexsha
    metric['sha'] = sha

    # TODO If the source files count is SUPER large(which should not happen usually), then we
    #  should go through the iterator and track the files count
    source_files = list(lizard.get_all_source_files([repo_dir], [], None))

    num_files = len(source_files)
    logger.info(f'{owner}/{repo_name}: {num_files} files to analyze')

    failed_files = []
    nloc, num_funcs = 0, 0
    # these 3 metrics are calculated by functions average
    func_nloc, token_count, cc = 0, 0, 0

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for file_path in source_files:
            futures.append(executor.submit(_analysis_single_file, file_path))

        for future in as_completed(futures):
            try:
                _nloc, _num_funcs, _func_nloc, _token_count, _cc = future.result(timeout=5)
                nloc += _nloc
                num_funcs += _num_funcs
                func_nloc += _func_nloc
                token_count += _token_count
                cc += _cc
            except TimeoutError:
                logger.error(f'analysis timeout: {file_path}, skip')
                failed_files.append(file_path)
            except TypeError as e:
                logger.error(f'Failed to analyze file {file_path}: {e}, skip')
                failed_files.append(file_path)

    metric['nloc'] = nloc
    metric['average_nloc'] = func_nloc / num_funcs if num_funcs else 0
    metric['average_cyclomatic_complexity'] = cc / num_funcs if num_funcs else 0
    metric['average_token_count'] = token_count / num_funcs if num_funcs else 0

    logger.info(f'{owner}/{repo_name} code repo metric: {metric}')

    return metric


class RepoCodeMetricRoutineCalculation(MetricRoutineCalculation):

    def calculate_metrics(self):
        logger.info(f'Calculating repo code metric for {self.owner}/{self.repo}')
        dir = f"{REPO_CLONE_DIR}/{self.owner}/{self.repo}"
        clone_(self.owner, self.repo, dir)

        calculated_metrics = extract_metrics_safe(self.owner, self.repo, dir) if os.path.exists(dir) else None
        # remove_repos(dir)
        return calculated_metrics

    def save_metrics(self):
        logger.info(f'Save code repo metrics with {len(self.batch)} records, to {self.table_name}')
        cursor = self.mysql_conn.cursor()
        insert_query = """
        INSERT INTO repo_code_metrics (owner, repo, created_at, sha,
        nloc, average_nloc, average_cyclomatic_complexity, average_token_count) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""
        cursor.executemany(insert_query, self.batch)
        self.mysql_conn.commit()
        logger.info(f"{len(self.batch)} records on {self.owner}/{self.repo} inserted successfully!")
        cursor.close()

    metrics_keys = ["owner", "repo", "created_at", "sha", "nloc", "average_nloc",
                    "average_cyclomatic_complexity", "average_token_count"]

    def routine_calculate_metrics_once(self):
        metrics = self.calculate_metrics()
        if not metrics:
            logger.warning(f'No metrics calculated for {self.owner}/{self.repo}, skip')
            return

        metrics_values = []
        for key in RepoCodeMetricRoutineCalculation.metrics_keys:
            metrics_values.append(metrics[key])
        self.batch = [metrics_values]
        self.save_metrics()
        self.clickhouse_client.close()
        self.mysql_conn.close()
