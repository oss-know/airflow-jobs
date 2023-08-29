import igraph as ig
import numpy as np
from scipy import stats


class Gragh:
    def __init__(self) -> None:
        self.graph = ig.Graph()
        self.committer_node_num = 0
        self.func_node_num = 0
        self.egonet = {}

    def create_graph_from_commit_func(self, commit_function: dict, function_index: dict, committer_index: dict):
        self.committer_node_num = len(committer_index)
        self.func_node_num = len(function_index)
        self.graph = [[0 for _ in range(self.committer_node_num + self.func_node_num)] for _ in range(self.committer_node_num + self.func_node_num)]
        for committer in commit_function:
            for function in commit_function[committer]:
                self.graph[committer_index[committer]][function_index[function]] = commit_function[committer][function]
                self.graph[function_index[function]][committer_index[committer]] = commit_function[committer][function]
        # print(commit_function)
        # print(self.graph)
        self.graph = ig.Graph.Adjacency(self.graph)
        # print(self.graph)

    def get_egonet(self):
        self.egonet = {k.index: list(set(self.graph.neighbors(k))) for k in self.graph.vs}

    def get_neighbor_num(self):
        neighbor_num = {k.index: len(self.egonet[k.index]) for k in self.graph.vs}
        return neighbor_num

    def cal_cluster_coef(self, vertex):
        L3_num, C4_num = 0, 0
        dev1_vertex = vertex.index
        func_vertices = self.egonet[dev1_vertex]
        for i in range(len(func_vertices)):
            dev2_vertices = self.egonet[func_vertices[i]].copy()
            dev2_vertices.remove(dev1_vertex)
            for dev2_vertex in dev2_vertices:
                for j in range(i + 1, len(func_vertices)):
                    # es1 = self.graph.es.select(_within=[dev1_vertex, func_vertices[i]])
                    # es2 = self.graph.es.select(_within=[dev2_vertex, func_vertices[i]])
                    num = len(self.graph.es.select(_within=[dev1_vertex, func_vertices[i]])) * len(self.graph.es.select(_within=[dev2_vertex, func_vertices[i]])) * len(self.graph.es.select(_within=[dev1_vertex, func_vertices[j]])) / 8
                    if self.graph.es.select(_within=[dev2_vertex, func_vertices[j]]):
                        C4_num += num
                        L3_num += 4 * num
                    else:
                        L3_num += num
        return 4 * C4_num / L3_num if L3_num != 0 else 0
        # subgraph_index = set(self.egonet[vertex.index] + [vertex.index])
        # for value in self.egonet[vertex.index]:
        #     subgraph_index.add(self.egonet[value])
        # subgraph_index = list(subgraph_index)
        # subgraph = self.graph.subgraph(subgraph_index)

    def get_cluster_coef(self):
        node_ccoef = {k.index: self.cal_cluster_coef(vertex=k) for k in self.graph.vs}
        return node_ccoef

    def get_two_hop_neighbor_num(self):
        two_hop_neighbors_num = {}
        for key in self.egonet:
            avg_hop_num = ig.mean([len(self.egonet[k]) - 1 for k in self.egonet[key]])
            two_hop_neighbors_num[key] = avg_hop_num
        return two_hop_neighbors_num

    def get_neighbor_cluster_coef(self):
        avg_neighbor_ccoef = {}
        node_ccoef = self.get_cluster_coef()
        for key in self.egonet:
            temp = ig.mean([node_ccoef[k] for k in self.egonet[key]])
            avg_neighbor_ccoef[key] = temp
        return avg_neighbor_ccoef

    def get_egonet_edge_num(self):
        egonet_edge_num = {}
        for vertex in self.graph.vs:
            subgraph = self.graph.subgraph(self.egonet[vertex.index] + [vertex.index])
            egonet_es = [(k.source, k.target) for k in subgraph.es]
            egonet_edge_num[vertex.index] = len(egonet_es) / 2  # Graph is undirected
        return egonet_edge_num

    def get_egonet_out_edge_num(self):
        egonet_out_edge_num = {}
        for vertex in self.graph.vs:
            total_vs = [vertex.index]
            for k in self.egonet[vertex.index]:
                total_vs = total_vs + self.egonet[k] + [k]
            total_vs = list(set(total_vs))
            subgraph = self.graph.subgraph(total_vs)
            # print(subgraph)
            total_es = [(k.source, k.target) for k in subgraph.es]
            subgraph_egonet = self.graph.subgraph(self.egonet[vertex.index] + [vertex.index])
            egonet_es = [(k.source, k.target) for k in subgraph_egonet.es]
            # egonet_out_edge_num[vertex.index] = len(list(set(total_es) - set(egonet_es)))
            egonet_out_edge_num[vertex.index] = (len(total_es) - len(egonet_es)) / 2  # Graph is undirected
        return egonet_out_edge_num

    def get_egonet_neighbor_num(self):
        egonet_neighbor_num = {}
        for vertex in self.graph.vs:
            egonet_vs = [vertex.index] + self.egonet[vertex.index]
            total_vs = []
            for k in self.egonet[vertex.index]:
                total_vs = total_vs + self.egonet[k]
            total_vs = list(set(total_vs))
            total_vs = [i for i in total_vs if i not in egonet_vs]
            egonet_neighbor_num[vertex.index] = len(total_vs)
        return egonet_neighbor_num

    def get_all_node_features(self):
        self.get_egonet()
        F1 = self.get_neighbor_num()
        F2 = self.get_cluster_coef()
        F3 = self.get_two_hop_neighbor_num()
        F4 = self.get_neighbor_cluster_coef()
        F5 = self.get_egonet_edge_num()
        F6 = self.get_egonet_out_edge_num()
        F7 = self.get_egonet_neighbor_num()
        all_features = [(F1[v.index], F2[v.index], F3[v.index], F4[v.index], F5[v.index], F6[v.index], F7[v.index]) for v in self.graph.vs]

        return all_features

    def get_graph_feature(self):
        all_node_features = self.get_all_node_features()
        # num_nodes = len(all_node_features)
        graph_feature_dev, graph_feature_func = [], []
        for k in range(0, 7):
            feat_agg_dev = [all_node_features[i][k] for i in range(0, self.committer_node_num)]
            feat_agg_func = [all_node_features[i][k] for i in range(self.committer_node_num, self.committer_node_num + self.func_node_num)]
            mn_dev, mn_func = ig.mean(feat_agg_dev), ig.mean(feat_agg_func)
            md_dev, md_func = ig.median(feat_agg_dev), ig.median(feat_agg_func)
            std_dev, std_func = np.std(feat_agg_dev), np.std(feat_agg_func)
            skw_dev, skw_func = stats.skew(feat_agg_dev), stats.skew(feat_agg_func)
            krt_dev, krt_func = stats.kurtosis(feat_agg_dev), stats.kurtosis(feat_agg_func)
            graph_feature_dev.append([mn_dev, md_dev, std_dev, skw_dev, krt_dev])
            graph_feature_func.append([mn_func, md_func, std_func, skw_func, krt_func])
        graph_feature = [graph_feature_dev, graph_feature_func]

        del all_node_features
        return graph_feature
