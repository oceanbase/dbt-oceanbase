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
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.oceanbase_mysql import OBMySQLAdapter
from dbt.adapters.oceanbase_mysql.impl import OBMySQLIndex
from dbt.adapters.oceanbase_mysql.relation import OBMySQLRelation
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


class TestListIndexed(BaseOBMySQLTestCase):

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
