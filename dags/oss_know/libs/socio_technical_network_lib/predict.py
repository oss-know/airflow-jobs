import sys
sys.path.append("/root/jy/airflow-jobs/dags/")

import random
import json
import torch
# from oss_know.libs.socio_technical_network_lib.Network import Trainer, train, test
from torch.utils.data import DataLoader
from oss_know.libs.util.log import logger
from oss_know.libs.socio_technical_network_lib.interact_with_opensearch import get_commit_data_from_opensearch, get_intermediate_data_from_opensearch, check_index
from oss_know.libs.socio_technical_network_lib.two_mode_model import TwoModeModel, Dataset
from oss_know.libs.socio_technical_network_lib.abstract_func import abstract_func
from oss_know.libs.util.base import get_opensearch_client
from opensearchpy import helpers

OPENSEARCH_DOXYGEN_RESULT = "doxygen_result"


def predict(owner, repo, opensearch_conn_info):
    alter_lines, lines_to_funcs = [], []

    check_index(opensearch_conn_info, OPENSEARCH_DOXYGEN_RESULT)
    opensearch_data = list(get_intermediate_data_from_opensearch(owner, repo, opensearch_conn_info))
    if len(opensearch_data) == 0: # If information of functions is not in opensearch, use abstract_func() to get from original repo files
        lines_to_funcs = abstract_func(owner, repo, opensearch_conn_info)
    else:
        for data in opensearch_data:
            lines_to_funcs.append(data["_source"]["raw_data"])

    commits = get_commit_data_from_opensearch(owner=owner, repo=repo, opensearch_conn_info=opensearch_conn_info)
    for commit in commits: # Get commit diff
        alter_lines.append(commit["_source"]["raw_data"])
    model = TwoModeModel(logger, alter_lines, lines_to_funcs)

    logger.info("Start generating graph features.")

    graph_feature = model.get_graph_features(repo)  # graph feature of all windows generated by repo
    logger.info("Finish generating graph features.")

    if len(graph_feature) == 0:
        logger.debug("Fail to calculate graph feature. Prediction Aborted.")
        logger.info("Project::{repo} is inactive.")
        return

    random.seed(2023)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.trainer.model.to(device)
    model.trainer.model.load_state_dict(torch.load("oss_know/libs/socio_technical_network_lib/out/model_weights.pth"))

    suc, fail = 0, 0
    model.trainer.model.eval()
    with torch.no_grad():
        for X in graph_feature:
            pred = model.trainer.model(X)
            if pred.argmax(1) == 1:
                suc += 1
            else:
                fail += 1
    if suc > fail:
        logger.info(f"Project::{repo} would be succeed in the future.")
        return
    else:
        logger.info(f"Project::{repo} would be fail in the future.")
        return


owner = "pytorch"
repo = "pytorch"
opensearch_conn_info = {"HOST": "123.57.177.158",
                        "PASSWD": "admin",
                        "PORT": "6792",
                        "USER": "admin"}
predict(owner, repo, opensearch_conn_info)