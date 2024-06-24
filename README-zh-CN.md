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
| 特性                  | 是否支持 | 备注   |
|:--------------------|:-----|:-----|
| Table               | ✅    | -    |
| View                | ✅    | -    |
| Incremental         | ✅    | -    |
| Epheneral           | ✅    | -    |
| Materializaed View  | ✅    | -    |
| Docs                | ✅    | 生成文档 |
| Seed                | ✅    | 导入数据 |
| Snapshot            | ✅    | 生成快照 |

### Table

dbt-oceanbase 对表对象进行了针对性的兼容，允许用户最大限度地使用 OceanBase 表地特性。

| 特性          | 是否支持 | 使用示例                                                                                                                                                                             | 说明                          |
|:------------|:-----|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:----------------------------|
| 外表          | ✅    | `{{ config(external=True) }}`                                                                                                                                                    | -                           |
| 列存          | ✅    | `{{ config(column_groups=['all columns', 'r_name(col1, col2)']) }}`                                                                                                              | -                           |
| 临时表         | ❌    | -                                                                                                                                                                                | OceanBase MySQL 模式暂不支持临时表   |
| contract    | ✅    | `{{ config(contract={'enforced': True}) }}`                                                                                                                                      | -                           |
| 检查约束（列/表）   | ✅    | `constraints.type='check'`                                                                                                                                                       | -                           |
| 非空约束（列/表）   | ✅    | `constraints.type='not_null'`                                                                                                                                                    | -                           |
| 唯一约束（列/表）   | ✅    | `constraints.type='unique'`                                                                                                                                                      | -                           |
| 主键约束（列/表）   | ✅    | `constraints.type='primary_key'`                                                                                                                                                 | -                           |
| 外键约束（表）     | ✅    | `constraints.type='foreign_key'`                                                                                                                                                 | -                           |
| 表级注释        | ✅    | `models.description='this is the comment'`                                                                                                                                       | -                           |
| 索引          | ✅    | `{{ config(indexes=[{"columns": ["id"],"algorithm": "BTREE", "unique": True, "options": ['GLOBAL'], "name": "idx", "column_groups": ['all columns', 'r_name(col1, col2)']}]) }}` | -                           |

### View

| 特性           | 是否支持  | 使用示例                                             | 说明                                                                             |
|:-------------|:------|:-------------------------------------------------|:-------------------------------------------------------------------------------|
| contract     | ✅     | `{{ config(contract={'enforced': True}) }}`      | -                                                                              |
| columns      | ✅     | `{{ config(columns=['col1', 'col2']) }}`         | -                                                                              |
| check option | ✅     | `{{ config(check_option="with check option") }}` | 可选项：`WITH CHECK OPTION`,`WITH CASCADED CHECK OPTION`,`WITH LOCAL CHECK OPTION` |
| 表级注释         | ❌     | -                                                | OceanBase MySQL 模式不支持添加视图注释                                                    |

### Materialized View

| 特性                       | 是否支持 | 使用示例                                                                                                                                                                             | 说明                                                                                                                                                       |
|:-------------------------|:-----|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------|
| full refresh             | ✅    | `{{ config(full_refresh=True) }}`                                                                                                                                                | 如果设置为 True，当环境中存在同名的物化视图时 dbt 将会删除旧的创建新的；如果设置为 False，dbt 将会对比新旧物化视图，并且将已存在的物化视图转化为新定义的                                                                   |
| table options            | ✅    | `{{ config(table_options=["comment='this is a comment'"]) }}`                                                                                                                    | -                                                                                                                                                        |
| columns                  | ✅    | `{{ config(columns=['col1', 'col2']) }}`                                                                                                                                         | -                                                                                                                                                        |
| refresh mode             | ✅    | `{{ config(refresh_mode="never refresh") }}`                                                                                                                                     | 物化视图刷新选项，可选值：`NEVER REFRESH`, `REFRESH (FAST\|COMPLETE\|FORCE) [ON (DEMAND\|COMMIT\|STATEMENT)] [[START WITH expr][NEXT expr]]`                          |
| check option             | ✅    | `{{ config(check_option="with check option") }}`                                                                                                                                 | 可选项：`WITH CHECK OPTION`,`WITH CASCADED CHECK OPTION`,`WITH LOCAL CHECK OPTION`                                                                           |
| 索引                       | ✅    | `{{ config(indexes=[{"columns": ["id"],"algorithm": "BTREE", "unique": True, "options": ['GLOBAL'], "name": "idx", "column_groups": ['all columns', 'r_name(col1, col2)']}]) }}` | -                                                                                                                                                        |
| on configuration change  | ✅    | `{{ config(on_configuration_change="apply") }}`                                                                                                                                  | 可选值：`apply`, `continue`, `fail`。该配置项在存在同名物化视图，`full_refresh`为 False 且存在的同名物化视图和新定义的存在差异时有效；`apply`：允许 dbt 将已存在的物化视图转换为新定义中，`continue`：忽略差异，`fail` 及其他：报错 |
| 表级注释                     | ❌    | -                                                                                                                                                                                | OceanBase MySQL 模式不支持针对物化视图添加注释                                                                                                                          |

### Incremental

物化方式 Incremental 是 Table 的扩展，因此 Incremental 天然支持所有的 Table 支持的特性。在这里我们只列出 Incremental 单独支持的属性配置：

| 特性                     | 是否支持 | 使用示例                                                 | 说明                                                                                                                                                                                |
|:-----------------------|:-----|:-----------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| unique key             | ✅    | `{{ config(unique_key="id") }}`                      | 只接受字符串类型的值。在 `incremental_strategy` 配置为 `delete+insert` 且环境中存在同名表对象时有效，将作为 where 条件中的唯一键删除已存在表中的数据                                                                                |
| full refresh           | ✅    | `{{ config(full_refresh=True) }}`                    | 如果设置为 True，当环境中存在同名的表时 dbt 将会删除旧的创建新的；如果设置为 False，dbt 将会对比新旧表对象，并且将已存在的表修改为新定义的                                                                                                   |
| on_schema_change       | ✅    | `{{ config(on_schema_change='sync_all_columns') }}`  | 可选值：`sync_all_columns`, `append_new_columns`, `fail`, `ignore`。`fail`: 已存在的同名表和新定义的表存在结构差异时报错，`append_new_columns`：已存在的同名表和新定义的表存在结构差异时只新增列，`sync_all_columns`：将已存在的同名表完全转化为新定义的表 |
| incremental_strategy   | ✅    | `{{ config(incremental_strategy='delete+insert') }}` | 可选值：`delete+insert`， `append`，`default`。`append` 以及 `default`：二者意义相同，每次运行时向已存在的表对象中追加全部数据；`delete+insert`：先删除已存在的同名表对象中的数据，再追加全部数据                                                |
| incremental_predicates | ✅    | `{{ config(incremental_predicates='a=b and c=d')}}`  | 当 `incremental_strategy` 为 `delete+insert` 有效，将在删除数据过程中辅助过滤数据                                                                                                                     |

物化类型 Incremental 在使用上需要一些技巧才能发挥出它的作用，详细的使用方式详见文档：[https://docs.getdbt.com/docs/build/incremental-models](https://docs.getdbt.com/docs/build/incremental-models)

### Snapshot

Snapshot（快照）是 dbt 中非常有用的特性之一，用于记录用户数据表中数据变化。有关 snapshot 的介绍及用法详见官方文档 [https://docs.getdbt.com/docs/build/snapshots](https://docs.getdbt.com/docs/build/snapshots)

| 特性                      | 是否支持 | 使用示例                                                                                                                                                                             | 说明                                                                              |
|:------------------------|:-----|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------------------------------------------------------------------------------|
| alias                   | ✅    | `{{ config(alias="snapshot_tbl_name") }}`                                                                                                                                        | 快照表的名字，如果不传则使用 dbt 生成的默认名字                                                      |
| strategy                | ✅    | `{{ config(strategy="check") }}`                                                                                                                                                 | 快照策略，只接受`check`以及`timestamp`                                                    |
| unique key              | ✅    | `{{ config(unique_key=['id']) }}`                                                                                                                                                | 唯一键，用于唯一定位一行记录的列集合，通常可以是表的主键或唯一键                                                |
| check cols              | ✅    | `{{ config(check_cols=['name']) }}`                                                                                                                                              | `strategy`为 check 时有效，用于记录快照表跟踪的列集合，只有在该集合中的列才会被 snapshot 功能记录                  |
| invalidate hard deletes | ✅    | `{{ config(invalidate_hard_deletes=True) }}`                                                                                                                                     | 是否把表的删除动作也纳入到快照的追踪范围内                                                           |
| updated at              | ✅    | `{{ config(updated_at='update_time')}}`                                                                                                                                          | 时间戳列，原表中用于描述记录修改时间的列                                                            |
| 索引                      | ✅    | `{{ config(indexes=[{"columns": ["id"],"algorithm": "BTREE", "unique": True, "options": ['GLOBAL'], "name": "idx", "column_groups": ['all columns', 'r_name(col1, col2)']}]) }}` | 用于给快照表增加索引                                                                      |
| target database         | ✅    | `{{ config(target_database='dbname') }}`                                                                                                                                         | 快照所在的 database，如果不传则为当前所在的 database                                             |
| target schema           | ✅    | `{{ config(target_schema='dbname') }}`                                                                                                                                           | 快照所在的 schema，由于 OceanBase MySQL 模式下只有 database 的概念，因此该值需要和 target_database 保持一致 |

