# dbt-oceanbase

[OceanBase Database](https://github.com/oceanbase/oceanbase) 是一个分布式关系型数据库。完全由蚂蚁集团自主研发。OceanBase 基于 [Paxos](https://lamport.azurewebsites.net/pubs/lamport-paxos.pdf) 协议以及分布式架构，实现了高可用和线性扩展，与此同时 OceanBase 还是一款 HTAP 数据库，可以运行联机分析型负载。OceanBase 数据库运行在常见的服务器集群上，不依赖特殊的硬件架构。

`dbt-oceanbase`将 OceanBase 与 DBT 集成在一起，允许用户使用 OceanBase 创建 DBT 项目或构建数据仓库。目前，本项目仅提供 OceanBase MySQL 兼容模式的支持，暂不支持 OceanBase Oracle 兼容模式。

## 安装

本项目使用 PyPi 进行管理，因此您需要预先安装 [setuptools](https://pypi.org/project/setuptools/)。您可以使用以下 shell 安装 dbt-oceanbase：
```shell
pip3 install dbt-oceanbase
```
目前兼容的 [dbt-core](https://github.com/dbt-labs/dbt-core) 最新版本为 1.8.0b2，我们将努力保证 dbt-oceanbase 与 dbt-core 最新版本之间的兼容性。欢迎您给我们提交 [Issues](https://github.com/oceanbase/dbt-oceanbase/issues) 以及 [PR](https://github.com/oceanbase/dbt-oceanbase/pulls)

## 快速开始

### 初始化

您可以通过以下 shell 脚本初始化您的 dbt 项目：

```shell
dbt init ${your project name}
```
您可以在交互式菜单中选择 oceanbase_mysql 来创建一个项目以在 OceanBase 上进行开发：
```shell
13:32:20  Running with dbt=1.8.0-b2
...
Happy modeling!

13:32:27  Setting up your profile.
Which database would you like to use?
[1] postgres
[2] oceanbase_mysql
```

### 配置您的 Profile

Profile 文件用于配置 dbt 项目，它将告诉 dbt-core 如何连接到 OceanBase 数据库并在上面执行您的 dbt 项目。您通常可以在`~/.dbt`目录下找到该文件。下面是 Profile 文件的一个例子：
```yaml
default:
  outputs:
    dev:
      type: oceanbase_mysql
      threads: 1
      host: 127.0.0.1
      port: 2881
      user: user@tenant#cluster
      pass: your_password
      schema: schema/database

  target: dev
```

| 选项      | 描述                                                  | 是否必须        | 举例                      |
|:--------|:----------------------------------------------------|:------------|:------------------------|
| type    | dbt 插件的名称，枚举值。只接受 oceanbase_mysql                   | 是           | oceanbase_mysql         |
| threads | 运行最大线程数，选填，默认为 1                                    | 否           | 1                       |
| host    | 数据库所在的 host 地址                                      | 是           | 127.0.0.1               |
| port    | 数据库所在的端口号                                           | 是           | 2881                    |
| user    | 数据库登录用户名，通常由用户名，租户名和集群名三部分组成                        | 是           | username@tenant#cluster |
| pass    | 数据库登录密码                                             | 是           | -                       |
| schema  | dbt 连接到的默认 schema（Oracle 模式） 或 database（MySQL 模式）   | 是（MySQL 模式） | test                    |

### 运行项目

初始化项目并且配置完成`Profile.yaml`文件之后，您可以通过以下命令检测项目的配置是否正常：
```shell
dbt debug
```
如果您能够正常连接到数据库，您可以通过以下命令运行您的第一个 dbt 项目，项目初始化时 dbt-core 已经为您创建了一个 demo 项目：

```shell
dbt run
```
如您看到如下输出，说明项目已经配置和运行成功：
```shell
13:39:29  Running with dbt=1.8.0-b2
13:39:30  Registered adapter: oceanbase_mysql=1.0.0
13:39:30  Found 2 models, 4 data tests, 1 snapshot, 411 macros
13:39:30  
13:39:32  Concurrency: 1 threads (target='dev')
13:39:32  
13:39:32  1 of 2 START sql table model test.my_first_dbt_model ......................... [RUN]
13:39:33  1 of 2 OK created sql table model test.my_first_dbt_model .................... [SELECT 2 in 0.74s]
13:39:33  2 of 2 START sql view model test.my_second_dbt_model ......................... [RUN]
13:39:33  2 of 2 OK created sql view model test.my_second_dbt_model .................... [CREATE VIEW in 0.53s]
13:39:34  
13:39:34  Finished running 1 table model, 1 view model in 0 hours 0 minutes and 3.42 seconds (3.42s).
13:39:34  
13:39:34  Completed successfully
13:39:34  
13:39:34  Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
```
您可以紧接着在 OceanBase 数据库上查看是否已经有名为 my_first_dbt_model 的表被创建以及名为 my_second_dbt_model 的视图被创建。

## 特性
| 特性                 | 是否支持 | 备注   |
|:-------------------|:-----|:-----|
| Table              | ✅    | -    |
| View               | ✅    | -    |
| Incremental        | ✅    | -    |
| Epheneral          |✅| -    |
| Materializaed View |✅| -    |
| Docs               |✅| 生成文档 |
| Seed               |✅| 导入数据 |
| Snaspshot          |✅| 生成快照 |

### Table

dbt-oceanbase 对表对象进行了针对性的兼容，允许用户最大限度地使用 OceanBase 表地特性。

| 特性        |是否支持| 使用示例                                                                                                                                                                            |
|:----------|:----|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 外表        | ✅ | ` {{ config(external=True) }}`                                                                                                                                                  |
| 列存        | ✅ | ` {{ config(column_groups=['all columns', 'r_name(col1, col2)']) }}`                                                                                                            |
| 临时表       | ✅ | ` {{ config(temporary=True) }}`                                                                                                                                                 |
| contract  | ✅ | ` {{ config(contract={'enforced': True}) }}`                                                                                                                                    |
| 检查约束（列/表） | ✅ | `constraints.type='check'`                                                                                                                                                      |
| 非空约束（列/表） | ✅ | `constraints.type='not_null'`                                                                                                                                                   |
| 唯一约束（列/表） | ✅ | `constraints.type='unique'`                                                                                                                                                     |
| 主键约束（列/表） | ✅ | `constraints.type='primary_key'`                                                                                                                                                |
| 外键约束（表）   | ✅ | `constraints.type='foreign_key'`                                                                                                                                                |
| 表级注释      | ✅ | `models.description='this is the comment'`                                                                                                                                      |
| 索引        | ✅ | `{{ config(indexes=[{"columns": ["id"],"algorithm": "BTREE", "unique": True, "options": ['GLOBAL'], "name": "idx", "column_groups": ['all columns', 'r_name(col1, col2)']}] }}` |

### View

| 特性           | 是否支持 | 使用示例                                              |
|:-------------|:-----|:--------------------------------------------------|
| contract     | ✅    | ` {{ config(contract={'enforced': True}) }}`      |
| columns      | ✅    | ` {{ config(columns=['col1', 'col2']) }}`         |
| check option | ✅    | ` {{ config(check_option="with check option") }}` |
| 表级注释      | ❌   | -                                                 |

### Materialized View

| 特性                      | 是否支持 | 使用示例                                                                                                                                                                            |
|:------------------------|:-----|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| full refresh            | ✅    | ` {{ config(full_refresh=True) }}`                                                                                                                                              |
| table options           | ✅    | ` {{ config(table_options=["comment='this is a comment'"]) }}`                                                                                                                  |
| columns                 | ✅    | ` {{ config(columns=['col1', 'col2']) }}`                                                                                                                                       |
| refresh mode            | ✅    | ` {{ config(refresh_mode="never refresh") }}`                                                                                                                                   |
| check option            | ✅    | ` {{ config(check_option="with check option") }}`                                                                                                                               |
| 索引                      | ✅ | `{{ config(indexes=[{"columns": ["id"],"algorithm": "BTREE", "unique": True, "options": ['GLOBAL'], "name": "idx", "column_groups": ['all columns', 'r_name(col1, col2)']}] }}` |
| on configuration change | ✅    | ` {{ config(on_configuration_change="apply") }}`                                                                                                                                |
| 表级注释                    | ❌   | -                                                                                                                                                                               |


