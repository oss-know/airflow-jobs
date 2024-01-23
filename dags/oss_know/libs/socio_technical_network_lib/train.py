import random
import json
import torch
from oss_know.libs.socio_technical_network_lib.abstract_func import abstract_func
from oss_know.libs.socio_technical_network_lib.Network import train, test
from torch.utils.data import DataLoader
from oss_know.libs.util.log import logger
from oss_know.libs.socio_technical_network_lib.interact_with_opensearch import check_index, get_commit_data_from_opensearch, get_intermediate_data_from_opensearch
from oss_know.libs.socio_technical_network_lib.two_mode_model import TwoModeModel, Dataset

OPENSEARCH_DOXYGEN_RESULT = "doxygen_result"


def train(owner, repo, opensearch_conn_info):
    '''
    If use train() to train the weights of network, dataset including labeled project should be prepared first.
    Every project in dataset need to label as 'success' or 'failure'.
    '''
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

    # If the labels corresponding to projects are put in seperate file 'class.json' under folder 'data', you could use this code segment. Otherwise, you should change the varible 'repo_class' on your way to get labels of projects.
    with open("oss_know/libs/socio_technical_network_lib/data/class.json") as f:
        repos = json.load(f)

    logger.info("Start generating graph features.")
    all_data, all_lable = [], []
    for repo in repos:
        graph_feature = model.get_graph_features(repo)
        all_data.extend(graph_feature)
        repo_class = [1 if repo["class"] == "success" else 0] * len(graph_feature)
        all_lable.extend(repo_class)

    logger.info("Finish generating graph features.")

    # divide dataset Random
    random.seed(2023)
    fold = 5
    logger.info("Divide train&test dataset by %d:%d" % (fold - 1, 1))
    total_num = len(all_data)
    train_index = random.sample(range(total_num), int(total_num * (fold - 1) / fold))
    trainset_fea, testset_fea, trainset_lable, testset_lable = [], [], [], []

    for i in range(total_num):
        if i in train_index:
            trainset_fea.append(all_data[i])
            trainset_lable.append(all_lable[i])
        else:
            testset_fea.append(all_data[i])
            testset_lable.append(all_lable[i])
    logger.info("Finish dividing dataset.")

    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.trainer.model.to(device)

    logger.info("Start training.")
    epoch_num = 100
    max_acc = 0
    for epoch in range(epoch_num):
        logger.info("Epoch: %d" % (epoch))
        train(DataLoader(Dataset(trainset_fea, trainset_lable), 8), model.trainer, device, logger)
        acc = test(DataLoader(Dataset(testset_fea, testset_lable), 8), model.trainer, device, logger)
        if acc > max_acc:
            logger.info(f"Better model with acc:{(acc):>4f}, saved in out/model_weights.pth")
            torch.save(model.trainer.model.state_dict(), 'oss_know/libs/socio_technical_network_lib/out/model_weights.pth')
            max_acc = acc
    logger.info(f"Best model was saved in out/model_weights.pth with acc:{(max_acc):>4f}")