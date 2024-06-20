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

from dbt.adapters.base import BaseAdapter
from dbt.cli.main import dbtRunner, dbtRunnerResult
from tests.functional.utils import BaseOBMySQLTestCase

_MODEL_EPHEMERAL_SQL = """

{{ config(materialized='ephemeral') }}

    select 1 as id union all select 2 as id

"""

_MODEL_TABLE_SQL = """

{{ config(materialized='table') }}
with cte as (
    select * from {{ ref('ephemeral_model')  }}
)
select * from cte

"""


class TestRunSimpleModel(BaseOBMySQLTestCase):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_first_model.sql": _MODEL_TABLE_SQL,
            "ephemeral_model.sql": _MODEL_EPHEMERAL_SQL,
        }

    def test_simple_model_run_succeed(self, project, dbt: dbtRunner):
        res: dbtRunnerResult = dbt.invoke(args=["run"])
        assert res.success == True
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            _, table = adapter.execute("show tables", fetch=True)
            assert ["my_first_model"] == [row.values()[0] for row in table.rows]
