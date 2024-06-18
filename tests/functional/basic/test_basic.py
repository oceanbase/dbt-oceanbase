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
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from tests.functional.utils import BaseOBMySQLTestCase

_MODEL_SQL = """

{{ config(
    persist_docs={"relation": True},
    materialized='table',
    indexes=[
        {
            "columns": ["id"],
        }
    ],
    contract={'enforced': True}) }}

    select 1 as id union all select 2 as id

"""

_MODEL_YML = """

models:
  - name: my_first_model
    config:
      materialized: table
      contract:
        enforced: true
    constraints:
      - type: unique
        columns: ['id']
    description: "this is comment"
    columns:
      - name: id
        quote: True
        data_type: bigint
        constraints:
          - type: not_null
          - type: unique

"""


class TestRunSimpleModel(BaseOBMySQLTestCase):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_first_model.sql": _MODEL_SQL,
            "my_first_model.yml": _MODEL_YML,
        }

    def test_simple_model_run_succeed(self, project, dbt: dbtRunner):
        res: dbtRunnerResult = dbt.invoke(args=["run"])
        assert res.success == True
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            _, table = adapter.execute("show tables", fetch=True)
            assert ["my_first_model"] == [row.values()[0] for row in table.rows]


class TestEmpty(BaseEmpty, BaseOBMySQLTestCase):
    pass
