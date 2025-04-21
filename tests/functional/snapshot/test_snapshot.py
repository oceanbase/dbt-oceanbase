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
import pytest

from dbt.adapters.base import BaseAdapter, BaseRelation
from dbt.tests.adapter.column_types.test_column_types import run_dbt
from tests.functional.utils import BaseOBMySQLTestCase

_TBL_NAME = "test_tbl"

_TS_PRE_HOOK = f"""

create table {_TBL_NAME} (
    id bigint primary key,
    status varchar(64),
    update_time timestamp default current_timestamp on update current_timestamp
);

insert into {_TBL_NAME}(id, status) values(1, 'Pending'), (2, 'Issued');
commit;

"""

_MODEL_SQL = f"""

{{{{ config(materialized='table') }}}}

    select * from {_TBL_NAME}

"""

_SNAPSHOT_DATABASE = "dbt_snapshots"

_SNAPSHOT_TS_STRATEGY_SQL = f"""
{{% snapshot my_first_model_snapshot %}}

{{{{ config(
    target_schema='{_SNAPSHOT_DATABASE}',
    target_database='{_SNAPSHOT_DATABASE}',
    strategy='timestamp',
    updated_at='update_time',
    unique_key='id',
    invalidate_hard_deletes=True) }}}}

SELECT * FROM {{{{ ref('my_first_model') }}}}

{{% endsnapshot %}}
"""

_SNAPSHOT_CHECK_STRATEGY_SQL = f"""
{{% snapshot my_first_model_snapshot %}}

{{{{ config(
    target_schema='{_SNAPSHOT_DATABASE}',
    target_database='{_SNAPSHOT_DATABASE}',
    strategy='check',
    check_cols=['id', 'status'],
    unique_key='id',
    invalidate_hard_deletes=True) }}}}

SELECT * FROM {{{{ ref('my_first_model') }}}}

{{% endsnapshot %}}
"""


class TestTimestampStrategy(BaseOBMySQLTestCase):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_first_model.sql": _MODEL_SQL,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "my_first_model_snapshot.sql": _SNAPSHOT_TS_STRATEGY_SQL,
        }

    @pytest.fixture(scope="class", autouse=True)
    def snapshot_schema(self, ob_mysql_connection) -> str:
        with ob_mysql_connection.cursor() as cursor:
            cursor.execute(f"create database if not exists {_SNAPSHOT_DATABASE}")
        yield _SNAPSHOT_DATABASE
        with ob_mysql_connection.cursor() as cursor:
            cursor.execute(f"drop database if exists {_SNAPSHOT_DATABASE}")

    def test_ts_col_exists_timestamp_strategy_create_succeed(self, project, snapshot_schema):
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            adapter.execute(_TS_PRE_HOOK)
        run_dbt(args=["run"])
        run_dbt(args=["snapshot"])
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            relation: BaseRelation = adapter.get_relation(
                snapshot_schema, snapshot_schema, "my_first_model_snapshot"
            )
            assert relation is not None and relation.is_table


class TestCheckStrategy(BaseOBMySQLTestCase):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_first_model.sql": _MODEL_SQL,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "my_first_model_snapshot.sql": _SNAPSHOT_CHECK_STRATEGY_SQL,
        }

    @pytest.fixture(scope="class", autouse=True)
    def snapshot_schema(self, ob_mysql_connection) -> str:
        with ob_mysql_connection.cursor() as cursor:
            cursor.execute(f"create database if not exists {_SNAPSHOT_DATABASE}")
        yield _SNAPSHOT_DATABASE
        with ob_mysql_connection.cursor() as cursor:
            cursor.execute(f"drop database if exists {_SNAPSHOT_DATABASE}")

    def test_check_strategy_create_succeed(self, project, snapshot_schema):
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            adapter.execute(_TS_PRE_HOOK)
        run_dbt(args=["run"])
        run_dbt(args=["snapshot"])
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            relation: BaseRelation = adapter.get_relation(
                snapshot_schema, snapshot_schema, "my_first_model_snapshot"
            )
            assert relation is not None and relation.is_table


class TestCheckStrategySnapshotTblExists(BaseOBMySQLTestCase):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_first_model.sql": _MODEL_SQL,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "my_first_model_snapshot.sql": _SNAPSHOT_CHECK_STRATEGY_SQL,
        }

    @pytest.fixture(scope="class", autouse=True)
    def snapshot_schema(self, ob_mysql_connection) -> str:
        with ob_mysql_connection.cursor() as cursor:
            cursor.execute(f"create database if not exists {_SNAPSHOT_DATABASE}")
        yield _SNAPSHOT_DATABASE
        with ob_mysql_connection.cursor() as cursor:
            cursor.execute(f"drop database if exists {_SNAPSHOT_DATABASE}")

    def test_check_strategy_snapshot_exists_create_succeed(self, project, snapshot_schema):
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            adapter.execute(_TS_PRE_HOOK)
        run_dbt(args=["run"])
        run_dbt(args=["snapshot"])
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            adapter.execute(f"insert into {_TBL_NAME}(id, status) values(3, 'Pending');")
            adapter.execute(f"delete from {_TBL_NAME} where id=2;")
            adapter.execute(f"update {_TBL_NAME} set status='Issued' where id=1;")
            adapter.execute(f"commit;")
        run_dbt(args=["run"])
        run_dbt(args=["snapshot"])
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            _, table = adapter.execute(
                f"select * from {snapshot_schema}.my_first_model_snapshot", fetch=True
            )
            actual = [(row["id"], row["status"], row["dbt_valid_to"] is None) for row in table]
            expect = [
                (1, "Pending", False),
                (2, "Issued", False),
                (1, "Issued", True),
                (3, "Pending", True),
            ]
            assert expect == actual


class TestTimestampStrategySnapshotTblExists(BaseOBMySQLTestCase):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_first_model.sql": _MODEL_SQL,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "my_first_model_snapshot.sql": _SNAPSHOT_TS_STRATEGY_SQL,
        }

    @pytest.fixture(scope="class", autouse=True)
    def snapshot_schema(self, ob_mysql_connection) -> str:
        with ob_mysql_connection.cursor() as cursor:
            cursor.execute(f"create database if not exists {_SNAPSHOT_DATABASE}")
        yield _SNAPSHOT_DATABASE
        with ob_mysql_connection.cursor() as cursor:
            cursor.execute(f"drop database if exists {_SNAPSHOT_DATABASE}")

    def test_check_strategy_snapshot_exists_create_succeed(self, project, snapshot_schema):
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            adapter.execute(_TS_PRE_HOOK)
        run_dbt(args=["run"])
        run_dbt(args=["snapshot"])
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            adapter.execute(f"insert into {_TBL_NAME}(id, status) values(3, 'Pending');")
            adapter.execute(f"delete from {_TBL_NAME} where id=2;")
            adapter.execute(f"update {_TBL_NAME} set status='Issued' where id=1;")
            adapter.execute(f"commit;")
        run_dbt(args=["run"])
        run_dbt(args=["snapshot"])
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            _, table = adapter.execute(
                f"select * from {snapshot_schema}.my_first_model_snapshot", fetch=True
            )
            actual = [(row["id"], row["status"], row["dbt_valid_to"] is None) for row in table]
            expect = [
                (1, "Pending", False),
                (2, "Issued", False),
                (1, "Issued", True),
                (3, "Pending", True),
            ]
            assert expect == actual
