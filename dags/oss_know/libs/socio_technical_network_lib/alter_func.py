# import json
import pandas as pd
from tqdm import tqdm


def get_committer_dict(commits):
    committer_dict = {}
    index = 0
    for commit in commits:
        committer = commit['committer_name']
        if committer not in committer_dict:
            committer_dict[committer] = index
            index += 1
    return committer_dict


def commit_func(repo, commits, lines_to_funcs):
    '''
    'commit_func' is dict of relations between committers and functions,
    whose first key is committer and second key is function

    'function_dict' is dict of functions and their corresponding index in graph

    'committer_dict' is dict of committer and their corresponding index in graph
    '''
    commit_func = {}
    function_dict = {}
    committer_dict = get_committer_dict(commits)
    func_index = len(committer_dict)
    lines_to_funcs = pd.DataFrame(lines_to_funcs)
    for commit in tqdm(commits):
        committer = commit['committer_name']
        # parents = commit['parents']
        # if not parents:
        #     continue
        # else:
            # for parent in parents:
            #     if parent not in commit['compare_with_parents']:
            #         continue
        alter_infos = commit['compare_with_parents']
        if committer not in commit_func:
            commit_func[committer] = {}
        for alter_info in alter_infos:
            try:
                filename = repo + alter_info['file_name']
            except Exception:
                filename = repo + alter_info['new_path']
            dummy_func = filename + "/dummy_function"
            alter_file_lines = alter_info['alter_file_lines']
            alter_funcs = set()
            # if filename in lines_to_funcs:
            #     for alter_file_line in alter_file_lines:
            #         file = lines_to_funcs[filename]
            #         if alter_file_line in file:
            #             alter_funcs.add(filename + '/' + file[alter_file_line])
            #         else:
            #             alter_funcs.add(dummy_func)
            # else:
            #     alter_funcs.add(dummy_func)
            for alter_file_line in alter_file_lines:
                file_exist = lines_to_funcs.loc[lines_to_funcs["file_path"] == filename]
                if not file_exist.empty:
                    alter_func = file_exist.loc[lines_to_funcs["lines"] == alter_file_line]
                else:
                    alter_funcs.add(dummy_func)
                    break
                if not alter_func.empty:
                    alter_funcs.add(filename + '/' + alter_func["func"].values[0])
                else:
                    alter_funcs.add(dummy_func)
            for alter_func in alter_funcs:
                if alter_func in commit_func[committer]:
                    commit_func[committer][alter_func] += 1
                else:
                    commit_func[committer][alter_func] = 1
                if alter_func not in function_dict:
                    function_dict[alter_func] = func_index
                    func_index += 1

    return commit_func, function_dict, committer_dict
