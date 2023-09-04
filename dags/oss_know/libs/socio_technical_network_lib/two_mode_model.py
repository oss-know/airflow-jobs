# import json
# import pandas as pd
import sys
from TimeSeries import TimeSeries
from alter_func import commit_func
from Graph import Gragh
from Network import Trainer
import torch
from torch.utils.data import IterableDataset


class TwoModeModel:
    def __init__(self, logger, alter_lines, lines_to_funcs) -> None:
        self.timeseries = TimeSeries()
        self.trainer = Trainer()
        self.log = logger
        self.alter_lines = alter_lines
        self.lines_to_funcs = lines_to_funcs

    def get_graph_features(self, repo, alpha=0.1):
        self.timeseries.set_series(self.alter_lines)
        self.timeseries.set_end(len(self.alter_lines) - 1, self.alter_lines[-1]['committed_date'])
        self.timeseries.set_slide_window(window_size=75, step=30, window_num=5)
        graph_feature_all = []
        try:
            self.timeseries.find_ealiest_window()
            windows = self.timeseries.generate_window()
        except Exception as e:
            self.log.debug(e)
            self.log.info("%s: Time series creation failed." % (repo))
            return []

        # graph_feature_all = torch.tensor(0)

        for i in range(self.timeseries.window_num):
            try:
                window_commits = next(windows)
            except Exception as e:
                self.log.info(e)
                self.log.info("%s: No more windows." % (repo))
                break

            commit_function, function_index, committer_index = commit_func(repo, window_commits, self.lines_to_funcs)
            graph = Gragh()
            graph.create_graph_from_commit_func(commit_function, function_index, committer_index)
            graph_feature = graph.get_graph_feature()
            graph_feature = graph_feature[0] + graph_feature[1]
            graph_feature_all.append(graph_feature)
            # graph_feature_all = alpha * graph_feature_all + graph_feature

        return graph_feature_all


class Dataset(IterableDataset):
    def __init__(self, features, lables) -> None:
        super().__init__()
        self.features = torch.tensor(features)
        self.lables = torch.tensor(lables)

    def __iter__(self):
        return zip(self.features, self.lables)

    def __len__(self):
        return len(self.features)
