#!/bin/bash

# grimoire 组件 grimoire-elk sortinghat cereslib 对 numpy pandas urllib3 的依赖混乱，请忽略
pip install --no-cache-dir -r requirements_010.txt -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host pypi.aliyun.com
pip install --no-cache-dir -r requirements_020.txt -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host pypi.aliyun.com
