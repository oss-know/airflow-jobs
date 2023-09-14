import json

import networkx as nx

from oss_know.libs.metrics.influence_metrics import MetricRoutineCalculation
from oss_know.libs.util.log import logger


class PrivilegeEventsMetricRoutineCalculation(MetricRoutineCalculation):
    privileged_events_list = ["added_to_project", "converted_note_to_issue",
                              "deployed", "deployment_environment_changed",
                              "locked", "merged", "moved_columns_in_project",
                              "pinned", "removed_from_project",
                              "review_dismissed", "transferred",
                              "unlocked", "unpinned", "user_blocked"]

    def calculate_metrics(self):
        logger.info(f'Calculating {self.owner}/{self.repo} privileged event metrics')
        event_type_index_map = {}
        for (index, event) in enumerate(self.privileged_events_list):
            event_type_index_map[event] = index

        privilege_sql_ = f"""
        WITH {PrivilegeEventsMetricRoutineCalculation.privileged_events_list} as privileged_events
        SELECT search_key__event, timeline_raw FROM github_issues_timeline 
        WHERE search_key__owner = '{self.owner}' AND search_key__repo = '{self.repo}'
        and has(privileged_events, search_key__event)
        """

        # TODO Even with constraints on search_key__event, an iterator is essential when data expands
        #  to a large scale.
        privilege_results = self.clickhouse_client.execute_no_params(privilege_sql_)
        response = []

        privilege_map = {}
        for (event_type, timeline_raw) in privilege_results:
            if event_type in self.privileged_events_list:
                raw_data = json.loads(timeline_raw)
                dev = raw_data['actor']
                dev_name = dev['login']
                if dev_name not in privilege_map:
                    privilege_map[dev_name] = [0] * 14
                index = event_type_index_map[event_type]
                privilege_map[dev_name][index] = 1

        for dev, events in privilege_map.items():
            row = [dev]
            row.extend(events)
            response.append(row)

        logger.info(f'{len(response)} Privilege Events Metrics calculated on {self.owner}/{self.repo}')
        return response

    def save_metrics(self):
        logger.info(f'Saving Privilege Events Metrics of {self.owner}/{self.repo}')
        # TODO The string literal can be stored as static class property
        privilege_events_insert_query = '''
            INSERT INTO privilege_events(actor_login, added_to_project, converted_note_to_issue,
                                  deployed, deployment_environment_changed,
                                  locked, merged, moved_columns_in_project,
                                  pinned, removed_from_project,
                                  review_dismissed, transferred,
                                  unlocked, unpinned, user_blocked)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)'''
        self.batch_insertion(insert_query=privilege_events_insert_query, batch=self.batch)


class CountMetricRoutineCalculation(MetricRoutineCalculation):
    def calculate_metrics(self):
        # TODO Add and handle author_email
        gits_sql_ = f"""
        SELECT author_name, count() AS commit_count, sum(total__lines) AS total_lines
        FROM gits
        WHERE search_key__owner = '{self.owner}'
          AND search_key__repo = '{self.repo}'
        GROUP BY author_name
        """
        gits_results = self.clickhouse_client.execute_no_params(gits_sql_)

        logger.info(f'Calculating Count Metrics of {self.owner}/{self.repo}')
        return gits_results

    def save_metrics(self):
        count_metrics_insert_query = f'''
            INSERT INTO {self.table_name}
            (search_key__owner, search_key__repo, author_name, commit_num, line_of_code)
            VALUES ("{self.owner}", "{self.repo}", %s, %s, %s)'''
        self.batch_insertion(insert_query=count_metrics_insert_query, batch=self.batch)
        logger.info(f'{len(self.batch)} Count Metrics saved for {self.owner}/{self.repo}')


class NetworkMetricRoutineCalculation(MetricRoutineCalculation):
    def files_first_chars(self):
        first_chars_sql = f'''
        SELECT DISTINCT substring(file__name, 1, 1) AS first_char
        FROM (
                 SELECT `files.file_name` AS file__name
                 FROM gits
                          ARRAY JOIN `files.file_name`
                 WHERE search_key__owner = '{self.owner}'
                   AND search_key__repo = '{self.repo}'
                 GROUP BY file__name)
        WHERE first_char != '';
        '''
        result = self.clickhouse_client.execute_no_params(first_chars_sql)
        return [tup[0] for tup in result]

    def developer_relation_sql(self, group_char, day_threshold=180):
        return f'''
        WITH '{self.owner}' AS owner, '{self.repo}' AS repo, '{group_char}' AS first_char
        SELECT a_author_name, b_author_name
        FROM (SELECT a.author_name   AS a_author_name,
                     a.authored_date AS a_authored_date,
                     b.author_name   AS b_author_name,
                     b.authored_date AS b_author_date,
                     file_name
              FROM (SELECT author_name, authored_date, `files.file_name` AS file_name
                    FROM gits
                             ARRAY JOIN `files.file_name`
                    WHERE search_key__owner = owner
                      AND search_key__repo = repo
                      AND length(parents) == 1
                      AND file_name != ''
                      AND substring(file_name, 1, 1) = first_char
                    ORDER BY file_name, authored_date) AS a GLOBAL
                       JOIN (SELECT author_name, authored_date, `files.file_name` AS file_name
                             FROM gits
                                      ARRAY JOIN `files.file_name`
                             WHERE search_key__owner = owner
                               AND search_key__repo = repo
                               AND length(parents) == 1
                               AND file_name != ''
                               AND substring(file_name, 1, 1) = first_char
                             ORDER BY file_name, authored_date) AS b ON a.file_name = b.file_name)
        WHERE a_author_name != b_author_name
          AND abs(toYYYYMMDD(a_authored_date) - toYYYYMMDD(b_author_date)) <= {day_threshold}
        GROUP BY a_author_name, b_author_name
        '''

    def calculate_metrics(self):
        logger.info(f'Calculate {self.owner}/{self.repo} developer network metrics')
        first_chars = self.files_first_chars()

        social_network = nx.Graph()
        for first_char in first_chars:
            developer_relations_sql = self.developer_relation_sql(first_char)
            developer_pairs = self.clickhouse_client.execute_no_params(developer_relations_sql)
            logger.info(f'{len(developer_pairs)} pairs on group {first_char}, {self.owner}/{self.repo}')
            for (dev1, dev2) in developer_pairs:
                social_network.add_edge(dev1, dev2)

        if nx.is_empty(social_network):
            logger.warning(f'No developer pair edges found for {self.owner}/{self.repo}, skip')
            return []

        eigenvector = nx.eigenvector_centrality(social_network, max_iter=300)
        response = []
        dev_nodes = list(social_network.nodes())
        for dev in dev_nodes:
            degree = social_network.degree(dev)
            response.append((self.owner, self.repo, dev, degree, eigenvector[dev]))

        node_num = social_network.number_of_nodes()
        edge_num = social_network.number_of_edges()
        log = f'Network Metrics of {self.owner}/{self.repo} calculated, {node_num} nodes and ' \
              f'{edge_num} edges in the graph'
        logger.info(log)

        return response

    def save_metrics(self):
        logger.info('saving  Network Metrics')
        network_metrics_insert_query = '''
            INSERT INTO developer_role_network_metrics
            (search_key__owner, search_key__repo,
            author_name, degree_centrality, eigenvector_centrality)
            VALUES (%s, %s, %s, %s, %s)'''
        self.batch_insertion(insert_query=network_metrics_insert_query, batch=self.batch)
