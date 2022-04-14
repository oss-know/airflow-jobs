# airflow-jobs

请注意: 项目默认分支为 development, 所有pr默认提交到 development 分支

[git 操作文档](./git-guide.md)

[clickhouse建表未明确字段自定义值记录](./clickhouse-customFields.md)

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
b.删除容器: docker-compose rm
c.更新docker镜像: docker pull airflowjobs/airflow:dev-python3.8
d.设置airflow用户: echo -e "AIRFLOW_UID=$(id -u)" > .env
e.查看airflow用户设置是否成功：ll -a--->cat .env
f.启动docker镜像: docker-compose up
```

# 关于os 数据插入或更新的时间戳
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

# 关于clickhouse 反推数据建表的数据提供
```
1、数据字段要求完整
2、数据要求明确，不能有null
3、数据中不能出现空列表 如[]
4、数据中如果出现[]中嵌套多层json结构需要说明提醒（一层的json可以），或者再嵌套列表也需要说明提醒
```

# 关于启动airflow需添加.env文件
```
在airflow-jobs目录下执行
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```

# 为不同的docker容器创建网络
```
docker network create -d bridge dev-network
```
