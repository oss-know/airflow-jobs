# git 基础操作指引

``` bash
git config user.email "xxx@mail.com"
git config user.name "xxxuser"
```

# 设置 git 代理
``` bash
git config http.proxy 'http://127.0.0.1:7890'
git config https.proxy 'http://127.0.0.1:7890'
```
# 取消 git 代理
``` bash
git config --unset http.proxy
git config --unset https.proxy
```

# 同步上游 参考步骤
## 第一步：查看所有远程库的远程地址
``` bash
git remote -v
```

## 第二步：添加源分支 URL
``` bash
git remote add upstream git@github.com:oss-know/airflow-jobs.git
```

## 第三步：检查所有远程库的远程地址
``` bash
git remote -v
```

## 第四步：从源分支获取最新的代码
``` bash
git fetch upstream
```

## 第五步：切换到主分支
``` bash
git checkout development
```

## 第六步：合并本地分支和源分支
``` bash
git merge upstream/development （更推荐 rebase）
git rebase upstream/development
```

## 第七步：Push 到 Fork 分支
``` bash
git push
```
