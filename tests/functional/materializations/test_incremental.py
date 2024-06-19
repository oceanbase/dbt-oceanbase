# Copyright (c) 2023 OceanBase.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from typing import Any, List

import pytest

from dbt.adapters.base import BaseAdapter, BaseRelation
from dbt.adapters.base.column import Column as BaseColumn
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.oceanbase_mysql.column import OBMySQLColumn
from dbt.adapters.oceanbase_mysql.relation import OBMySQLRelation
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.tests.adapter.column_types.test_column_types import run_dbt
from tests.functional.utils import BaseOBMySQLTestCase

_ALL_TYPE_TBL_NAME = "all_type_test_tbl"

_ALL_TYPE_TBL_DDL = f"""
create table `{_ALL_TYPE_TBL_NAME}` (
    `col1` tinyint(4) default null,
    `col2` smallint(6) not null,
    `col3` mediumint(9) default null,
    `col4` int(11) default null,
    `col5` bigint(20) default null,
    `col6` bigint(20) unsigned default null,
    `col7` decimal(10,3) default null,
    `col8` float default null,
    `col9` float(5,3) default null,
    `col10` char(256),
    `col11` varchar(512) default null,
    `col12` tinytext default null,
    `col13` text default null,
    `col14` mediumtext default null,
    `col15` longtext default null,
    `col16` tinyblob default null,
    `col17` blob default null,
    `col18` mediumblob default null,
    `col19` longblob default null,
    `col20` binary(128) default null,
    `col21` varbinary(256) default null,
    `col22` date default null,
    `col23` timestamp(5) default null,
    `col24` time(2) default null,
    `col25` datetime default null,
    `col26` year(4) default null,
    `col27` bit(16) default null
);
"""


class TestListColumns(BaseOBMySQLTestCase):

    def test_all_type_exists_list_succeed(self, project, dbt_profile_target):
        adapter: BaseAdapter = project.adapter
        database = dbt_profile_target["database"]
        with adapter.connection_named("test"):
            adapter.execute(_ALL_TYPE_TBL_DDL)
            actual: List[dict[str:Any]] = [
                {
                    "column": item,
                    "data_type": item.data_type,
                    "is_string": item.is_string(),
                    "is_number": item.is_number(),
                    "is_float": item.is_float(),
                    "is_integer": item.is_integer(),
                    "is_numeric": item.is_numeric(),
                    "string_size": item.string_size() if item.is_string() else -1,
                }
                for item in adapter.get_columns_in_relation(
                    OBMySQLRelation.create(
                        database, database, _ALL_TYPE_TBL_NAME, RelationType.Table
                    )
                )
            ]
            expect = [
                {
                    "column": OBMySQLColumn("col1", "tinyint", None, 4, 0),
                    "data_type": "tinyint",
                    "is_string": False,
                    "is_number": True,
                    "is_float": False,
                    "is_integer": True,
                    "is_numeric": False,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col2", "smallint", None, 6, 0),
                    "data_type": "smallint",
                    "is_string": False,
                    "is_number": True,
                    "is_float": False,
                    "is_integer": True,
                    "is_numeric": False,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col3", "mediumint", None, 9, 0),
                    "data_type": "mediumint",
                    "is_string": False,
                    "is_number": True,
                    "is_float": False,
                    "is_integer": True,
                    "is_numeric": False,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col4", "int", None, 11, 0),
                    "data_type": "int",
                    "is_string": False,
                    "is_number": True,
                    "is_float": False,
                    "is_integer": True,
                    "is_numeric": False,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col5", "bigint", None, 20, 0),
                    "data_type": "bigint",
                    "is_string": False,
                    "is_number": True,
                    "is_float": False,
                    "is_integer": True,
                    "is_numeric": False,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col6", "bigint", None, 20, 0),
                    "data_type": "bigint",
                    "is_string": False,
                    "is_number": True,
                    "is_float": False,
                    "is_integer": True,
                    "is_numeric": False,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col7", "decimal", None, 10, 3),
                    "data_type": "decimal(10,3)",
                    "is_string": False,
                    "is_number": True,
                    "is_float": False,
                    "is_integer": False,
                    "is_numeric": True,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col8", "float", None, 12, None),
                    "data_type": "float",
                    "is_string": False,
                    "is_number": True,
                    "is_float": True,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col9", "float", None, 5, 3),
                    "data_type": "float",
                    "is_string": False,
                    "is_number": True,
                    "is_float": True,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col10", "char", 256, None, None),
                    "data_type": "character varying(256)",
                    "is_string": True,
                    "is_number": False,
                    "is_float": False,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": 256,
                },
                {
                    "column": OBMySQLColumn("col11", "varchar", 512, None, None),
                    "data_type": "character varying(512)",
                    "is_string": True,
                    "is_number": False,
                    "is_float": False,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": 512,
                },
                {
                    "column": OBMySQLColumn("col12", "tinytext", 255, None, None),
                    "data_type": "character varying(255)",
                    "is_string": True,
                    "is_number": False,
                    "is_float": False,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": 255,
                },
                {
                    "column": OBMySQLColumn("col13", "text", 65535, None, None),
                    "data_type": "character varying(65535)",
                    "is_string": True,
                    "is_number": False,
                    "is_float": False,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": 65535,
                },
                {
                    "column": OBMySQLColumn("col14", "mediumtext", 16777215, None, None),
                    "data_type": "character varying(16777215)",
                    "is_string": True,
                    "is_number": False,
                    "is_float": False,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": 16777215,
                },
                {
                    "column": OBMySQLColumn("col15", "longtext", 536870910, None, None),
                    "data_type": "character varying(536870910)",
                    "is_string": True,
                    "is_number": False,
                    "is_float": False,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": 536870910,
                },
                {
                    "column": OBMySQLColumn("col16", "tinyblob", 255, None, None),
                    "data_type": "tinyblob",
                    "is_string": False,
                    "is_number": False,
                    "is_float": False,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col17", "blob", 65535, None, None),
                    "data_type": "blob",
                    "is_string": False,
                    "is_number": False,
                    "is_float": False,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col18", "mediumblob", 16777215, None, None),
                    "data_type": "mediumblob",
                    "is_string": False,
                    "is_number": False,
                    "is_float": False,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col19", "longblob", 536870910, None, None),
                    "data_type": "longblob",
                    "is_string": False,
                    "is_number": False,
                    "is_float": False,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col20", "binary", 128, None, None),
                    "data_type": "binary",
                    "is_string": False,
                    "is_number": False,
                    "is_float": False,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col21", "varbinary", 256, None, None),
                    "data_type": "varbinary",
                    "is_string": False,
                    "is_number": False,
                    "is_float": False,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col22", "date", None, None, None),
                    "data_type": "date",
                    "is_string": False,
                    "is_number": False,
                    "is_float": False,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col23", "timestamp", None, None, None),
                    "data_type": "timestamp",
                    "is_string": False,
                    "is_number": False,
                    "is_float": False,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col24", "time", None, None, None),
                    "data_type": "time",
                    "is_string": False,
                    "is_number": False,
                    "is_float": False,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col25", "datetime", None, None, None),
                    "data_type": "datetime",
                    "is_string": False,
                    "is_number": False,
                    "is_float": False,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col26", "year", None, None, None),
                    "data_type": "year",
                    "is_string": False,
                    "is_number": False,
                    "is_float": False,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": -1,
                },
                {
                    "column": OBMySQLColumn("col27", "bit", None, 16, 0),
                    "data_type": "bit",
                    "is_string": False,
                    "is_number": False,
                    "is_float": False,
                    "is_integer": False,
                    "is_numeric": False,
                    "string_size": -1,
                },
            ]
            assert expect == actual


_MODEL_SQL = """

{{ config(
    full_refresh=True,
    pre_hook=["create table test_tbl(id bigint, name varchar(64), gender tinyint)"],
    materialized='incremental') }}

    select * from test_tbl

"""


class TestRunSimpleModel(BaseOBMySQLTestCase):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_first_model.sql": _MODEL_SQL,
        }

    def test_simple_model_run_succeed(self, project, dbt: dbtRunner):
        res: dbtRunnerResult = dbt.invoke(args=["run"])
        assert res.success == True
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            _, table = adapter.execute("show tables", fetch=True)
            assert ["my_first_model", "test_tbl"] == [row[0] for row in table]


class TestExistingRelationExists(BaseOBMySQLTestCase):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_first_model.sql": _MODEL_SQL,
        }

    def test_full_refresh_on_and_existing_relation_exists_create_succeed(
        self, project, dbt_profile_target
    ):
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            sql = """
            create table my_first_model (
                id bigint,
                name varchar(64)
            )
            """
            adapter.execute(sql)
        run_dbt(args=["run"])
        adapter: BaseAdapter = project.adapter
        database = dbt_profile_target["database"]
        with adapter.connection_named("test"):
            relations: List[BaseRelation] = adapter.list_relations_without_caching(
                OBMySQLRelation.create(database, database, None, None)
            )
            actual = [item for item in relations if item.type == RelationType.Table]
            expect = [
                OBMySQLRelation.create(database, database, "my_first_model", RelationType.Table),
                OBMySQLRelation.create(database, database, "test_tbl", RelationType.Table),
            ]
            assert expect == actual


_MODEL_NO_FULL_REFRESH_SQL = """

{{ config(
    full_refresh=False,
    pre_hook=["create table test_tbl(id bigint, name varchar(64), gender tinyint)"],
    materialized='incremental',
    on_schema_change='sync_all_columns') }}

    select * from test_tbl

"""


class TestSyncAllColsExistingRelationExists(BaseOBMySQLTestCase):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_first_model.sql": _MODEL_NO_FULL_REFRESH_SQL,
        }

    def test_full_refresh_off_and_existing_relation_exists_create_succeed(
        self, project, dbt_profile_target
    ):
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            sql = """
            create table my_first_model (
                id float(5, 3),
                name varchar(32),
                notice text
            )
            """
            adapter.execute(sql)
        run_dbt(args=["run"])
        adapter: BaseAdapter = project.adapter
        database = dbt_profile_target["database"]
        with adapter.connection_named("test"):
            actual: BaseRelation = adapter.get_relation(database, database, "my_first_model")
            expect = OBMySQLRelation.create(
                database, database, "my_first_model", RelationType.Table
            )
            assert expect == actual
            actual: List[BaseColumn] = adapter.get_columns_in_relation(actual)
            expect = [
                OBMySQLColumn("name", "varchar", 64, None, None),
                OBMySQLColumn("gender", "tinyint", None, 4, 0),
                OBMySQLColumn("id", "bigint", None, 20, 0),
            ]
            assert expect == actual


_MODEL_NO_FULL_REFRESH_DEL_INSERT_SQL = """

{{ config(
    full_refresh=False,
    pre_hook=[
        "create table test_tbl(id bigint, name varchar(64), gender tinyint);", 
        "insert into test_tbl(id,name,gender) values(1, 'Marry', 1), (2, 'David', 0)",
    ],
    materialized='incremental',
    on_schema_change='sync_all_columns',
    incremental_strategy='delete+insert',
    unique_key="id") }}

    select * from test_tbl

"""


class TestSyncAllColsDeleteAndInsert(BaseOBMySQLTestCase):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_first_model.sql": _MODEL_NO_FULL_REFRESH_DEL_INSERT_SQL,
        }

    def test_using_del_and_insert_strategy_create_succeed(self, project, dbt_profile_target):
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            sql = """
            create table my_first_model (
                id float(5, 3),
                name varchar(32),
                notice text
            )
            """
            adapter.execute(sql)
        run_dbt(args=["run"])
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            _, table = adapter.execute("select * from my_first_model", fetch=True)
            actual = [[col for col in row] for row in table]
            expect = [["Marry", 1, 1], ["David", 0, 2]]
            assert expect == actual
