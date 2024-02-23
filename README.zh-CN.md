<img src="https://github.com/tapdata/tapdata-private/raw/master/assets/logo-orange-grey-bar.png" width="300px"/>
<p align="center">
    <a href="https://github.com/tapdata/tapdata/graphs/contributors" alt="Contributors">
        <img src="https://img.shields.io/github/contributors/tapdata/tapdata" /></a>
    <a href="https://github.com/tapdata/tapdata/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/tapdata/tapdata" /></a>
    <a href="https://tapdata.github.io/tapdata">
        <img src="https://github.com/tapdata/tapdata/actions/workflows/docker-image.yml/badge.svg" alt="build status"></a>
</p>

[中文简要文档地址](https://github.com/tapdata/tapdata/blob/master/README.zh-CN.md)

[English Readme](https://github.com/tapdata/tapdata/blob/master/README.md)

## 完整在线文档地址: https://tapdata.github.io/
## Tapdata 为什么而存在
Tapdata 是新一代的实时数据平台, 通过把企业核心数据实时集中到中央化数据平台的方式并通过API 或者反向同步方式, 为下游的交互式应用, 微服务或交互式分析提供新鲜实时的数据

你也可以使用Tapdata作为一个实时数据集成（ETL）工具, 提供各种异构数据库之间的实时复制能力

## Tapdata 架构图 
<img src="https://github.com/tapdata/tapdata/raw/master/assets/tapdata-ldp.png" width="900px"/>

## 安装准备
### 环境准备
1. 在开始之前, 请保证您的环境安装了 Docker
2. 当前工具仅在 Linux 下进行过完整测试, 其他操作系统的适配正在进行中
3. 克隆当前仓库代码到本地: `git clone https://github.com/tapdata/tapdata.git && cd tapdata`

### 快速启动
1. 执行 `bash build/quick-use.sh` 会快速启动一个使用环境, 然后会自动进入 tapshell 交互客户端
2. 下次进入环境时, 可执行 `bash bin/tapshell.sh` 进入交互命令行工具
 
### 从源码编译启动
1. 执行 `bash build/quick-dev.sh` 会从源码编译, 并启动一个完整的使用环境,  然后会自动进入 tapshell 交互客户端
2. 下次进入环境时, 可执行 `bash bin/tapshell.sh` 进入交互命令行工具

### 环境清理
1. 执行 `bash build/clean.sh` 会清理包括编译中间产物, 编译镜像, 运行容器在内的全部内容, 但是会保留运行的任务配置与进度等信息
2. 如果需要删除任务运行配置, 请删除主目录的 data 目录即可

## 使用说明
1. 在环境启动后, 可通过 `bash bin/tapshell.sh` 进入交互客户端

交互客户端可使用命令模式, 或者 Shell API 模式进行实时数据平台的使用

### 基本概念
1. 数据连接器: 平台支持的数据连接类型, 比如 Mysql, PG, MongoDB
2. 数据源: 通过连接器创建的具体的数据来源
3. 数据表: 具有一定数据结构的数据集合
4. 任务: 

### 查看资源
1. 查看支持的数据连接器
```
>>> show connectors
1839d8 MongoDB
183a77 Mysql
183af5 PostgreSQL
```

2. 查看创建的数据源
```
>>> show datasources
id         status     database_type        name
183afa     ready      MongoDB              mongodb
```

3. 查看数据源下的表
```
>>> use mongodb
datasource switch to: mongodb

>>> show tables
```

4. 查看数据表的结构
```
>>> use mongodb
datasource switch to: mongodb

>>> desc CAR_CLAIM
{
    "_id": "OBJECT_ID",
    "SETTLED_DATE": "DATE_TIME",
    "CLAIM_ID": "STRING",
    "SETTLED_AMOUNT": "INT32",
    "CLAIM_REASON": "STRING",
    "POLICY_ID": "STRING",
    "CLAIM_DATE": "DATE_TIME",
    "LAST_CHANGE": "DATE_TIME",
    "CLAIM_AMOUNT": "INT32"
}
```

5. 查看任务列表, 分别为 任务id, 名字, 状态, 类型
```
>>> show jobs
system has 3 jobs
18415a: migrate                                    running      sync/initial_sync+cdc
1843e1: migrate2                                   error        sync/initial_sync+cdc
```

### 操作数据源
```
# mongodb
>>> source = DataSource("mongodb", "$name").uri("$uri")

# mysql
>>> source = DataSource("mysql", "$name").host("$host").port($port).username("$username").port($port).db("$db")

# pg
>>> source = DataSource("postgres", "$name").host("$host").port($port).username("$username").port($port).db("$db").schema("$schema").logPluginName("wal2json")

# 保存数据源, 并加载表结构
>>> source.save()

# 重新加载表结构
>>> validate datasource $name

# 删除数据源
>>> delete datasource $name
```

### 操作任务
1. 同步一张表, 默认为全量+增量同步
```
# 创建一个工作流
>>> p = Pipeline("$name")

# 使用 readFrom 从源读取数据, 使用 writeTo 将其写向目标
>>> p.readFrom("$source_name.$table").writeTo("$sink_name.$table")

# 启动任务
>>> p.start()

# 监控工作流的任务, 查看指标与日志
>>> p.monitor()
>>> p.logs()

# 停止任务
>>> p.stop()

# 列出任务
>>> show jobs

# 监控任务, 查看指标与日志
>>> monitor job $name
>>> logs job $name [tail=False] [limit=10] [t=30]

# 停止任务
>>> stop job $name

# 删除任务
>>> delete job $name
```

2. 同步一张表, 并使用自定义函数进行一些简单数据处理, 目前你可以使用 Python3 语法来进行函数的定义
```
# 1. 定义一个方法, 对 record 进行变换, 并返回 record
>>> def fn(record):
        record["x"] = 1
        return record

# 2. 使用 processor 算子指定变换方法
>>> p.readFrom(...).processor(fn).writeTo(...)
```

3. 同步多张表
```
# 创建一个工作流
>>> p = Pipeline("$name")

# 新建一个包含多张数据表的数据读取源, 支持正则匹配
>>> source = Source("$datasource_name", ["table1", "table2"...])
>>> source = Source("$datasource_name", table_re="xxx.*")

# 通过 writeTo 方法, 可修改同步表的前后缀
>>> p.readFrom(source).writeTo("$datasource_name", prefix="", suffix="")
```


## 开源 License
Tapdata 使用 Apache V2 License

## 加入我们
- 微信
<img src="https://github.com/tapdata/tapdata/raw/master/assets/wechat-qr-code.jpg" width="300px"/>

- [Slack](https://join.slack.com/t/tapdatacommunity/shared_invite/zt-1biraoxpf-NRTsap0YLlAp99PHIVC9eA)

