import datetime
import os
import shutil

import git  # GitPython
import lizard

from oss_know.libs.base_dict.variable_key import REPO_CLONE_DIR
from oss_know.libs.metrics.influence_metrics import MetricRoutineCalculation
from oss_know.libs.util.log import logger


def clone_(owner, repo, dir):
    url = f"https://github.com/{owner}/{repo}.git"

    try:
        if os.path.exists(dir):
            repo = git.Repo(dir)
            o = repo.remotes.origin
            o.pull()
        else:
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


class RepoCodeMetricRoutineCalculation(MetricRoutineCalculation):

    def calculate_metrics(self):
        logger.info(f'Calculating repo code metric for {self.owner}/{self.repo}')
        dir = f"{REPO_CLONE_DIR}/{self.owner}/{self.repo}"
        clone_(self.owner, self.repo, dir)

        calculated_metrics = extract_metrics(self.owner, self.repo, dir) if os.path.exists(dir) else None
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
