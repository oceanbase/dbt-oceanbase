# dbt-oceanbase

[OceanBase Database](https://github.com/oceanbase/oceanbase) is a native distributed database, independently developed by Ant Group. It leverages the [Paxos](https://lamport.azurewebsites.net/pubs/lamport-paxos.pdf) protocol and a distributed architecture to achieve high availability and linear scalability. Additionally, OceanBase is an HTAP (Hybrid Transactional and Analytical Processing) database capable of handling both online transactional and analytical workloads. It runs on common server clusters without relying on specialized hardware.

The `dbt-oceanbase` adapter integrates OceanBase with DBT, allowing users to create DBT projects or build data warehouses on OceanBase. Currently, this project only supports OceanBase databases in the MySQL mode. Support for the Oracle mode is not yet available.

## Installation

This project is managed via PyPi, so you will need to have [setuptools](https://pypi.org/project/setuptools/) installed in advance. You can install dbt-oceanbase using the following shell command:

```shell
pip3 install dbt-oceanbase
```

Alternatively, you can install dbt-oceanbase from the source code. First, clone the repository locally, then navigate to the project directory and run:

```shell
pip3 install .
```

The current compatible version of [dbt-core](https://github.com/dbt-labs/dbt-core) is 1.8.0b2. We aim to maintain compatibility between dbt-oceanbase and the latest versions of dbt-core. Contributions in the form of [Issues](https://github.com/oceanbase/dbt-oceanbase/issues) and [PRs](https://github.com/oceanbase/dbt-oceanbase/pulls) are welcomed.

## Quick Start

### Initialization

You can initialize your dbt project using the following shell command:

```shell
dbt init ${your_project_name}
```

During the setup, select `obmysql` to create a project for development on OceanBase:

```shell
13:32:20    Running with dbt=1.8.0-b2
...
Happy modeling!

13:32:27    Setting up your profile.
Which database would you like to use?
[1] postgres
[2] obmysql
```

### Configure Your Profile

The `Profile` file configures your dbt project, guiding dbt-core on how to connect to the OceanBase database and execute your dbt project. This file is typically located in the `~/.dbt` directory. Below is a sample Profile configuration:

```yaml
default:
    outputs:
        dev:
            type: obmysql
            threads: 1
            host: 127.0.0.1
            port: 2881
            user: user@tenant#cluster
            pass: your_password
            schema: schema/database

    target: dev
```

| Option  | Description                      | Required | Example                                          |
|---------|-----------------------|----------|-----------------------------|
| type    | The dbt plugin name, enumerated value. Only `obmysql` is accepted.    | Yes            | obmysql                          |
| threads | Maximum number of threads to run, optional, default is 1.                                      | No              | 1                                                      |
| host    | The host address where the database is located.                                                          | Yes            | 127.0.0.1                                      |
| port    | The port number where the database is located.                                                            | Yes            | 2881                                                |
| user    | The database login username, typically composed of username, tenant, and cluster.| Yes            | username@tenant#cluster          |
| pass    | The database login password.                                                                                                | Yes            | -                                                      |
| schema  | The default schema (Oracle mode) or database (MySQL mode) that dbt connects to.| Yes (MySQL mode) | test                                              |

### Run the Project

After initializing the project and configuring the `Profile.yaml` file, you can verify the project configuration with the following command:

```shell
dbt debug
```

If the connection to the database is successful, you can run your first dbt project with:

```shell
dbt run
```

During the initialization of the project, `dbt-core` has already created a demo project for you.

If you see the following output, it indicates that the project has been successfully configured and executed:

```shell
13:39:29    Running with dbt=1.8.0-b2
13:39:30    Registered adapter: obmysql=1.0.1
13:39:30    Found 2 models, 4 data tests, 1 snapshot, 411 macros
13:39:30    
13:39:32    Concurrency: 1 threads (target='dev')
13:39:32    
13:39:32    1 of 2 START sql table model test.my_first_dbt_model ......................... [RUN]
13:39:33    1 of 2 OK created sql table model test.my_first_dbt_model .................... [SELECT 2 in 0.74s]
13:39:33    2 of 2 START sql view model test.my_second_dbt_model ......................... [RUN]
13:39:33    2 of 2 OK created sql view model test.my_second_dbt_model .................... [CREATE VIEW in 0.53s]
13:39:34    
13:39:34    Finished running 1 table model, 1 view model in 0 hours 0 minutes and 3.42 seconds (3.42s).
13:39:34    
13:39:34    Completed successfully
13:39:34    
13:39:34    Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
```

You can then check in the OceanBase database to see if a table named `my_first_dbt_model` and a view named `my_second_dbt_model` have been created.

## Features
| Feature           | Supported | Notes                                      |
|-------------------|-----------|-------------------------|
| Table             | ✅                  | -                                              |
| View              | ✅                  | -                                              |
| Incremental       | ✅                  | -                                              |
| Ephemeral         | ✅                  | -                                              |
| Materialized View | ✅                  | -                                              |
| Docs              | ✅                  | Documentation Generation|
| Seed              | ✅                  | Data Import                          |
| Snapshot          | ✅                  | Snapshot Creation              |

### Table

`dbt-oceanbase` provides compatibility with table objects, allowing users to maximize the use of OceanBase's table features.

| Feature                                | Supported | Example                    | Description                                                            |
|----------------------------------------|-----------|------------------|------------------------------------------|
| Columnar Storage                       | ✅                  | `{{ config(column_groups=['all columns', 'r_name(col1, col2)']) }}`      | -                    |
| Temporary Table                        | ❌                  | -          | Temporary tables are not supported in OceanBase MySQL mode |
| Contract                               | ✅                  | `{{ config(contract={'enforced': True}) }}`              | -                                                    |
| Check Constraints (Column/Table)       | ✅                  | `constraints.type='check'`                    | -                                          |
| Not Null Constraints  (Column/Table)   | ✅                  | `constraints.type='not_null'`            | -                                                                |
| Unique Constraints  (Column/Table)     | ✅                  | `constraints.type='unique'`                        | -                                                              |
| Primary Key Constraints (Column/Table) | ✅                  | `constraints.type='primary_key'`                        | -                                                                                |
| Foreign Key Constraints (Table)        | ✅                  | `constraints.type='foreign_key'`                      | -                                                                                |
| Table-level Comments                   | ✅                  | `models.description='this is the comment'`                | -                                                                                |
| Indexes                                | ✅                  | `{{ config(indexes=[{"columns": ["id"],"algorithm": "BTREE", "unique": True, "options": ['GLOBAL'], "name": "idx", "column_groups": ['all columns', 'r_name(col1, col2)']}]) }}` | -                                                                                |

### View

| Feature              | Supported    | Example      | Description            |
|----------------------|------------|------------|------------------|
| Contract             | ✅                    | `{{ config(contract={'enforced': True}) }}`                  | -          |
| Columns              | ✅                    | `{{ config(columns=['col1', 'col2']) }}`            | -            |
| Check Option         | ✅                    | `{{ config(check_option="with check option") }}`          | Options: `WITH CHECK OPTION`, `WITH CASCADED CHECK OPTION`, `WITH LOCAL CHECK OPTION` |
| Table-level Comments | ❌                    | -        | OceanBase MySQL mode does not support view comments            |

### Materialized View

| Feature                 | Supported | Example            | Description                    |
|-------------------------|---------|-------------------------|-----------------------------|
| Full Refresh            | ✅                | `{{ config(full_refresh=True) }}`                | When set to `True`, dbt will drop and recreate the materialized view if one with the same name already exists; if `False`, dbt will compare the old and new materialized views and convert the existing one to the new definition. |
| Table Options           | ✅                | `{{ config(table_options=["comment='this is a comment'"]) }}`                | -                        |
| Columns                 | ✅                | `{{ config(columns=['col1', 'col2']) }}`                        | -                          |
| Refresh Mode            | ✅                | `{{ config(refresh_mode="never refresh") }}`                          | Refresh options for the materialized view, including `NEVER REFRESH`, `REFRESH (FAST\|COMPLETE\|FORCE) [ON (DEMAND\|COMMIT\|STATEMENT)] [[START WITH expr][NEXT expr]]`.          |
| Check Option            | ✅                | `{{ config(check_option="with check option") }}`        | Options include `WITH CHECK OPTION`, `WITH CASCADED CHECK OPTION`, and `WITH LOCAL CHECK OPTION`.                  |
| Indexes                 | ✅                | `{{ config(indexes=[{"columns": ["id"],"algorithm": "BTREE", "unique": True, "options": ['GLOBAL'], "name": "idx", "column_groups": ['all columns', 'r_name(col1, col2)']}]) }}` | -                      |
| On Configuration Change | ✅                | `{{ config(on_configuration_change="apply") }}`            | Options include `apply`, `continue`, and `fail`. Effective when there is an existing materialized view with the same name and `full_refresh` is set to `False`, but the existing and new definitions differ. <ul><li>`apply`: converts the existing materialized view to the new definition.</li><li>`continue`: ignores differences.</li><li>`fail` and others: throws an error.</li></ul> |
| Table-level comments    | ❌                | -                | OceanBase MySQL mode does not support adding comments to materialized views.        |

### Incremental

The incremental materialization is an extension of the table materialization, meaning that it naturally supports all features supported by the table materialization. Below are the specific properties that incremental alone supports:

| Feature                | Supported | Example            | Description                |
|------------------------|---------|--------------|-----------------|
| Unique Key             | ✅                | `{{ config(unique_key="id") }}`                                                      | Accepts string values only. Effective when `incremental_strategy` is set to `delete+insert` and there is an existing table with the same name. It will act as a unique key in the `where` condition to delete data from the existing table. |
| Full Refresh           | ✅                | `{{ config(full_refresh=True) }}`                                                  | When set to `True`, dbt will drop and recreate the table if one with the same name already exists; if `False`, dbt will compare the old and new table objects and modify the existing one to match the new definition.    |
| On Schema Change       | ✅                | `{{ config(on_schema_change='sync_all_columns') }}`              | Options include `sync_all_columns`, `append_new_columns`, `fail`, and `ignore`. <ul><li>`fail`: throws an error if there are structural differences between the existing table and the new definition.</li><li>`append_new_columns`: only adds new columns if there are differences.</li><li>`sync_all_columns`: completely converts the existing table to the new definition.</li></ul> |
| incremental_strategy   | ✅                | `{{ config(incremental_strategy='delete+insert') }}`            | Options include `delete+insert`, `append`, and `default`. `append` and `default` have the same meaning — each run appends all data to the existing table object; `delete+insert`: first deletes data from the existing table object and then appends all data. |
| incremental_predicates | ✅                | `{{ config(incremental_predicates='a=b and c=d')}}`              | Effective when `incremental_strategy` is set to `delete+insert`, helps filter data during the deletion process.          |

Using the incremental materialization effectively requires certain techniques. For detailed usage, refer to [the documentation](https://docs.getdbt.com/docs/build/incremental-models).

### Snapshot

Snapshot is one of the most useful features in dbt, allowing users to track changes in tables of user data. For more information on snapshot usage, refer to the [official documentation](https://docs.getdbt.com/docs/build/snapshots).

| Feature                 | Supported | Example              | Description                    |
|-------------------------|---------|------------|---------|
| Alias                   | ✅                | `{{ config(alias="snapshot_tbl_name") }}`                | Name of the snapshot table. If not provided, dbt will generate a default name.                    |
| Strategy                | ✅                | `{{ config(strategy="check") }}`      | Snapshot strategy, accepts only `check` and `timestamp`.      |
| Unique Key              | ✅                | `{{ config(unique_key=['id']) }}`              | Unique key, used to uniquely identify a row of data, typically the primary key or unique key of the table.                                                  |
| Check Cols              | ✅                | `{{ config(check_cols=['name']) }}`            | Effective when `strategy` is `check`, specifies the set of columns to be tracked by the snapshot. Only columns in this set will be tracked. |
| Invalidate Hard Deletes | ✅                | `{{ config(invalidate_hard_deletes=True) }}`        | Whether to track deletion actions in the table as part of the snapshot.              |
| Updated At              | ✅                | `{{ config(updated_at='update_time')}}`              | The timestamp column, describes the column in the original table that records the modification time of the record.                                  |
| Indexes                 | ✅                | `{{ config(indexes=[{"columns": ["id"],"algorithm": "BTREE", "unique": True, "options": ['GLOBAL'], "name": "idx", "column_groups": ['all columns', 'r_name(col1, col2)']}]) }}` | Used to add indexes to the snapshot table.      |
| Target Database         | ✅                | `{{ config(target_database='dbname') }}`        | The database where the snapshot is located. If not provided, it defaults to the current database.                                                                    |
| Target Schema           | ✅                | `{{ config(target_schema='dbname') }}`              | The schema where the snapshot is located. Since OceanBase MySQL mode only has the concept of databases, this value should be consistent with `target_database`. |