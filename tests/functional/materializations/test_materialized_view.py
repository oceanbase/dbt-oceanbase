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
from typing import List

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.base import BaseAdapter, BaseRelation
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.oceanbase_mysql import OBMySQLAdapter
from dbt.adapters.oceanbase_mysql.impl import OBMySQLIndex
from dbt.adapters.oceanbase_mysql.relation import OBMySQLRelation
from dbt.tests.adapter.column_types.test_column_types import run_dbt
from tests.functional.utils import BaseOBMySQLTestCase

_CREATE_STMT = """
create table test_tbl(
    id bigint,
    name varchar(20),
    age int(11)
);
create index idx_id_name_test_tbl on test_tbl(id, name);
create unique index idx_id_unique_test_tbl on test_tbl(id);
"""


class TestListIndexes(BaseOBMySQLTestCase):

    def test_indexes_exists_list_succeed(self, project, dbt_profile_target):
        database = dbt_profile_target["database"]
        adapter: OBMySQLAdapter = project.adapter
        with adapter.connection_named("test"):
            adapter.execute(_CREATE_STMT)
            relation: OBMySQLRelation = OBMySQLRelation.create(
                database, database, "test_tbl", RelationType.Table
            )
            actual = adapter.list_indexes(relation)
            expect = [
                OBMySQLIndex(
                    name="idx_id_name_test_tbl",
                    columns=["id", "name"],
                    unique=False,
                    algorithm="BTREE",
                ),
                OBMySQLIndex(
                    name="idx_id_unique_test_tbl", columns=["id"], unique=True, algorithm="BTREE"
                ),
            ]
            assert expect == actual

    def test_no_indexes_exists_exp_thrown(self, project, dbt_profile_target):
        with pytest.raises(DbtRuntimeError):
            database = dbt_profile_target["database"]
            adapter: OBMySQLAdapter = project.adapter
            with adapter.connection_named("test"):
                relation: OBMySQLRelation = OBMySQLRelation.create(
                    database, database, "test_tbl1", RelationType.Table
                )
                actual = adapter.list_indexes(relation)
                assert len(actual) == 0


_SIMPLE_MATERIALIZED_VIEW = """

{{ config(
    refresh_mode="NEVER REFRESH",
    check_option="WITH CHECK OPTION",
    materialized='materialized_view',
    full_refresh=False,
    indexes=[
        {
            "columns": ["id"],
            "unique": True,
        },
        {
            "columns": ["id", "name"],
        }
    ],
    pre_hook=["create table test_tbl(id bigint, name varchar(64))"])}}

select * from test_tbl

"""


class TestSimpleMaterializedView(BaseOBMySQLTestCase):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_first_model.sql": _SIMPLE_MATERIALIZED_VIEW,
        }

    def test_no_exists_view_create_succeed(self, project, dbt_profile_target):
        run_dbt(args=["run"])
        database = dbt_profile_target["database"]
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            relations: List[BaseRelation] = adapter.list_relations_without_caching(
                OBMySQLRelation.create(database, database, None, None)
            )
            actual = [item for item in relations if item.type == RelationType.MaterializedView]
            expect = [
                OBMySQLRelation.create(
                    database, database, "my_first_model", RelationType.MaterializedView
                )
            ]
            assert expect == actual


_PRE_MATERIALIZED_VIEW_EXISTS = """

{{ config(
    refresh_mode="NEVER REFRESH",
    check_option="WITH CHECK OPTION",
    materialized='materialized_view',
    full_refresh=True)}}

select * from test_tbl

"""


class TestPreMaterializedViewExists(BaseOBMySQLTestCase):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_first_model.sql": _PRE_MATERIALIZED_VIEW_EXISTS,
        }

    def test_exists_pre_m_view_create_succeed(self, project, dbt_profile_target):
        adapter: BaseAdapter = project.adapter
        database = dbt_profile_target["database"]
        with adapter.connection_named("test"):
            sql = """
            create table test_tbl(
                id bigint,
                name varchar(64)
            );

            create materialized view my_first_model as select * from test_tbl;
            """
            adapter.execute(sql)
        run_dbt(args=["run"])
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            relations: List[BaseRelation] = adapter.list_relations_without_caching(
                OBMySQLRelation.create(database, database, None, None)
            )
            actual = [item for item in relations if item.type == RelationType.MaterializedView]
            expect = [
                OBMySQLRelation.create(
                    database, database, "my_first_model", RelationType.MaterializedView
                )
            ]
            assert expect == actual


_INDEX_CHANGES = """

{{ config(
    refresh_mode="NEVER REFRESH",
    check_option="WITH CHECK OPTION",
    materialized='materialized_view',
    full_refresh=False,
    indexes=[
        {
            "name": "idx_1",
            "columns": ["id"],
            "unique": True,
        },
        {
            "name": "idx_2",
            "columns": ["id", "name"],
        }
    ])}}

select * from test_tbl

"""


class TestPreMaterializedViewExistsAndIndexChange(BaseOBMySQLTestCase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_first_model.sql": _INDEX_CHANGES,
        }

    def test_exists_pre_m_view_and_idx_change_create_succeed(self, project, dbt_profile_target):
        adapter: BaseAdapter = project.adapter
        database = dbt_profile_target["database"]
        with adapter.connection_named("test"):
            sql = """
                create table test_tbl(
                    id bigint,
                    name varchar(64)
                );

                create materialized view my_first_model as select * from test_tbl;
                
                create index idx_1 on my_first_model(id, name);
                create unique index idx_2 on my_first_model(id);
                """
            adapter.execute(sql)
        run_dbt(args=["run"])
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            relations: List[BaseRelation] = adapter.list_relations_without_caching(
                OBMySQLRelation.create(database, database, None, None)
            )
            actual = [item for item in relations if item.type == RelationType.MaterializedView]
            expect = [
                OBMySQLRelation.create(
                    database, database, "my_first_model", RelationType.MaterializedView
                )
            ]
            assert expect == actual


_INDEX_CHANGES_NOT_ALLOW_CHANGE = """

{{ config(
    refresh_mode="NEVER REFRESH",
    check_option="WITH CHECK OPTION",
    materialized='materialized_view',
    full_refresh=False,
    indexes=[
        {
            "name": "idx_1",
            "columns": ["id"],
            "unique": True,
        },
        {
            "name": "idx_2",
            "columns": ["id", "name"],
        }
    ],
    on_configuration_change="fail")}}

select * from test_tbl

"""


class TestPreMaterializedViewExistsAndNotAllowChange(BaseOBMySQLTestCase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_first_model.sql": _INDEX_CHANGES_NOT_ALLOW_CHANGE,
        }

    def test_exists_pre_m_view_and_idx_change_create_succeed(self, project, dbt_profile_target):
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            sql = """
                create table test_tbl(
                    id bigint,
                    name varchar(64)
                );

                create materialized view my_first_model as select * from test_tbl;

                create index idx_1 on my_first_model(id, name);
                create unique index idx_2 on my_first_model(id);
                """
            adapter.execute(sql)
        with pytest.raises(AssertionError):
            run_dbt(args=["run"])
