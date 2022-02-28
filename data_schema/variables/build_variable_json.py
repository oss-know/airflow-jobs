import datetime
import json


def timestamp_to_utc(timestamp):
    # 10位时间戳
    return datetime.datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%dT%H:%M:%SZ")


file = open('projectlist', 'r', encoding='utf8')
line = file.readline()
count = 0
project_list = []
while line:
    if line.endswith("\n"):
        project_list.append(line[0:-1])
    else:
        project_list.append(line)
    line = file.readline()
file.close()
with open('variable.json', 'r') as file:
    variable = json.load(file)
    now_time = timestamp_to_utc(int(datetime.datetime.now().timestamp()))
    list1 = []
    list2 = []
    list3 = []
    for project in project_list:
        owner_and_repo = project[19:].split("/")
        owner = owner_and_repo[0]
        repo = owner_and_repo[1]
        dict1 = {"owner": owner, "repo": repo, "since": "1970-01-01T00:00:00Z",
                 "until": now_time}
        list1.append(dict1)
        dict2 = {"owner": owner, "repo": repo}
        list2.append(dict2)
        dict3 = {"owner": owner, "repo": repo, "url": f"{project}.git"}
        list3.append(dict3)
    for key in variable:
        if key == 'need_init_github_commits_repos':
            variable[key] = list1
        elif key == 'need_init_gits' or key == 'need_sync_gits':
            variable[key] = list3
        elif key.startswith('need_init') or key.startswith('need_sync'):
            variable[key] = list2
with open('big_variable.json', 'w', encoding='utf8') as file:
    json.dump(variable, file, indent=4)
