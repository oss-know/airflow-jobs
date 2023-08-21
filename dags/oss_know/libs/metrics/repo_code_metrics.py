import datetime
import json
from collections import defaultdict
from statistics import mean
from oss_know.libs.util.log import logger

import lizard
from lizard import OutputScheme

from oss_know.libs.metrics.influence_metrics import MetricRoutineCalculation

import os
import shutil
import git # GitPython

def clone_one_depth(owner, repo, dir, update=True):
    url = f"https://github.com/{owner}/{repo}.git"

    if os.path.exists(os.path.join(dir, '.git')):
        if update:
            shutil.rmtree(dir)
            os.rmdir(dir)
        return
    try:
        git.Repo.clone_from(url, dir, depth=1)
    except git.exc.GitError as e:
        print(f'ERROR! {url} : {str(e)}')
        return 


def remove_repos(dir):
    if os.path.exists(dir):
        shutil.rmtree(dir)
        os.rmdir(dir)


def extract_metrics(owner, repo_name, repo_dir):
    metric = {}
    metric['owner'] = owner
    metric['repo'] = repo_name
    metric['created_at'] = datetime.datetime.now()

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
        logger.info(f'calculating by {self.owner}, {self.repo}')
        dir = f"./{self.owner}/{self.repo}"
        clone_one_depth(self.owner, self.repo, dir, True)
        calculated_metrics = extract_metrics(self.owner, self.repo, dir)
        remove_repos(dir)
        return calculated_metrics

    def save_metrics(self):
        logger.info(f'save metrics with {len(self.batch)} records, to {self.table_name}')
        cursor = self.mysql_conn.cursor()
        insert_query = "INSERT INTO repo_code_metrics (owner, repo, created_at, sha, nloc, average_nloc, average_cyclomatic_complexity, average_token_count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
        cursor.executemany(insert_query, self.batch)
        self.mysql_conn.commit()
        logger.info("Data inserted successfully!")
        cursor.close()

    def routine_calculate_metrics_once(self):
        metrics = self.calculate_metrics()
        metrics_keys = ["owner", "repo", "created_at", "sha", "nloc", "average_nloc", "average_cyclomatic_complexity", "average_token_count"]
        metrics_values = []
        for key in metrics_keys:
            metrics_values.append(metrics[key])
        self.batch = [metrics_values]
        self.save_metrics()
        self.clickhouse_client.close()
        self.mysql_conn.close()
