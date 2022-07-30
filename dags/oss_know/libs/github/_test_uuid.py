# -*-coding:utf-8-*-
import uuid
def get_uuid():
    uuid_list = []
    for i in range(100000):
        uuid_list.append(str(uuid.uuid1()))
    print(uuid_list)
    return list(set(uuid_list))