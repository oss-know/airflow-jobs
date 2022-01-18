# airflow-jobs
airflow-jobs

# Dag 测试同步 github commit
```
{
    "owner" : "apache",
    "repo" : "tomcat",
    "since" : "1980-01-01T00:00:00Z",
    "until" : "2021-12-12T00:00:00Z"
}
```

# Dag 测试同步 github commit
```
{
    "HSOT" : "apache",
    "PORT" : "tomcat",
    "USER" : "1980-01-01T00:00:00Z",
    "PASSWD" : "2021-12-12T00:00:00Z"
}
```

#需要设置更多的token

# 更新docker镜像
```
a.停止容器运行: docker-compose stop
b.删除容器: docker-conpose rm
c.更新docker镜像: docker pull airflowjobs/airflow:dev-python3.8
d.设置airflow用户: echo -e "AIRFLOW_UID=$(id -u)" > .env
e.查看airflow用户设置是否成功：ll -a--->cat .env
f.启动docker镜像: docker-compose up
```

# 关于os 数据插入或更新时间
```
search_key{
    .....
    "updated_at":"时间戳 本条记录更新(包含插入)的时间戳"
    'updated_at': int(datetime.datetime.now().timestamp() * 1000)
}
要保证时间戳为13位整数

```
# 关于os中的日期格式
```
所有os中的时间都是 2022-01-16T16:21:23Z,时区单独存储,原始时间字符串单独存储
```

