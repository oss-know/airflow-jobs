import datetime
import os
import shutil

from git import Repo
from pydriller import Repository

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_GIT_MODIFY_FILE_RAW
from oss_know.libs.github.init_gits import delete_old_data
from oss_know.libs.util.base import get_opensearch_client, now_timestamp
from oss_know.libs.util.log import logger
from oss_know.libs.util.opensearch_api import OpensearchAPI


def get_timezone_zero_time(dt):
    # 将时间转换为0时区
    utc_dt = dt.astimezone(datetime.timezone.utc)

    # 格式化为字符串表示形式
    utc_time_str = utc_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    return utc_time_str


def get_modified_lines(owner, repo, path, opensearch_client):
    bulk_datas = []
    count = 0
    opensearch_api = OpensearchAPI()
    for commit in Repository(path).traverse_commits():
        try:

            raw = {
                "owner": owner,
                "repo": repo,
                "hexsha": commit.hash,
                "parents": commit.parents,
                "author_tz": -int(commit.author_timezone / 60 / 60),
                "committer_tz": -int(commit.committer_timezone / 60 / 60),
                "author_name": commit.author.name,
                "author_email": commit.author.email,
                "committer_name": commit.committer.name,
                "committer_email": commit.committer.email,
                "authored_date": get_timezone_zero_time(commit.author_date),
                "authored_timestamp": int(commit.author_date.timestamp()),
                "committed_date": get_timezone_zero_time(commit.committer_date),
                "committed_timestamp": int(commit.author_date.timestamp()),
                "compare_with_parents": [],
                "deletions": commit.deletions,
                "insertions": commit.insertions,
                "lines": commit.lines,
                "files": commit.files
            }
            if len(commit.parents) > 1:
                continue

            for m in commit.modified_files:

                file_diff_parsed = m.diff_parsed
                added = []
                for line_num, _ in file_diff_parsed["added"]:
                    added.append(line_num)
                deleted = []
                for line_num, _ in file_diff_parsed["deleted"]:
                    deleted.append(line_num)
                alter_file_lines = list(set(added + deleted))
                alter_file_lines.sort()
                old_path = m.old_path
                new_path = m.new_path
                if old_path:
                    old_path = '/' + old_path
                if new_path:
                    new_path = '/' + new_path
                if old_path == new_path and new_path:
                    file_name = new_path
                elif not old_path:
                    file_name = new_path
                elif not new_path:
                    file_name = old_path
                elif not new_path and not old_path and new_path != old_path:
                    file_name = new_path

                raw["compare_with_parents"].append(
                    {"old_path": old_path,
                     "new_path": new_path,
                     "file_name": file_name,
                     "insertion_lines": added,
                     "deletion_lines": deleted,
                     "alter_file_lines": alter_file_lines})
            bulk_data_tp = {
                "_index": OPENSEARCH_GIT_MODIFY_FILE_RAW,
                "_source": {
                    "search_key": {
                        "owner": owner, "repo": repo, 'updated_at': now_timestamp()
                    },
                    "raw_data": raw
                }
            }
            bulk_datas.append(bulk_data_tp)
            count += 1
            if len(bulk_datas) % 500 == 0:
                success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client,
                                                                    bulk_all_data=bulk_datas,
                                                                    owner=owner, repo=repo)
                logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
                logger.info(f"count:{count}::{owner}/{repo}")
                bulk_datas.clear()
            if count == 2000:
                break

        except Exception as e:
            print(e)
    if bulk_datas:
        success, failed = opensearch_api.do_opensearch_bulk(opensearch_client=opensearch_client, bulk_all_data=bulk_datas,
                                                            owner=owner, repo=repo)
        logger.info(f"sync_bulk_git_datas::success:{success},failed:{failed}")
        logger.info(f"count:{count}::{owner}/{repo}")


def init_gits_repo_modify_file(git_url, owner, repo, proxy_config, opensearch_conn_datas, git_save_local_path=None):
    # 克隆版本库
    repo_path = f'{git_save_local_path["PATH"]}/{owner}/{repo}'
    if os.path.exists(repo_path):
        shutil.rmtree(repo_path)

    Repo.clone_from(url=git_url, to_path=repo_path, config=proxy_config)

    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_datas)
    # 删除在数据库中已经存在的此项目数据

    delete_old_data(owner=owner, repo=repo, index=OPENSEARCH_GIT_MODIFY_FILE_RAW, client=opensearch_client)

    # 获取每个commit中每个文件的修改情况 只取前2000条，不足取全量
    get_modified_lines(owner=owner, repo=repo, path=repo_path, opensearch_client=opensearch_client)


    return
