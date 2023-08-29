import random
import json
import torch
from oss_know.libs.socio_technical_network_lib.Network import train, test
from torch.utils.data import DataLoader
from oss_know.libs.util.log import logger
from oss_know.libs.socio_technical_network_lib.interact_with_opensearch import get_commit_data_from_opensearch, get_intermediate_data_from_opensearch
from oss_know.libs.socio_technical_network_lib.two_mode_model import TwoModeModel, Dataset


def train(owner, repo, opensearch_conn_info):
    commits = get_commit_data_from_opensearch(owner=owner, repo=repo, opensearch_conn_info=opensearch_conn_info)
    alter_lines, lines_to_funcs = [], []
    for commit in commits:
        alter_lines.append(commit["_source"]["raw_data"])
    opensearch_data = get_intermediate_data_from_opensearch(owner, repo, opensearch_conn_info)
    for data in opensearch_data:
        lines_to_funcs.append(data["_source"]["raw_data"]["lines_to_funcs"])

    model = TwoModeModel(logger, alter_lines, lines_to_funcs)
    with open("TimeSeries/data/class.json") as f:
        repos = json.load(f)

    logger.info("Start generating graph features.")
    all_data, all_lable = [], []
    for repo in repos:
        graph_feature = model.get_graph_features(repo)
        all_data.extend(graph_feature)
        repo_class = [1 if repo["class"] == "success" else 0] * len(graph_feature)
        all_lable.extend(repo_class)

    with open("features.json", 'w') as f:
        json.dump({"features": all_data, "labels": all_lable}, f, indent=4)


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
            torch.save(model.trainer.model.state_dict(), 'TimeSeries/out/model_weights.pth')
            max_acc = acc
    logger.info(f"Best model was saved in out/model_weights.pth with acc:{(max_acc):>4f}")