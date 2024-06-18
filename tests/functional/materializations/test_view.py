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

from dbt.adapters.base import BaseAdapter, BaseRelation
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.oceanbase_mysql.relation import OBMySQLRelation
from dbt.tests.adapter.column_types.test_column_types import run_dbt
from tests.functional.utils import BaseOBMySQLTestCase

_MODEL_SQL = """

{{ config(materialized='view', contract={'enforced': True}) }}

    select 1 as id union all select 2 as id

"""

_MODEL_CHAR_YML = """

models:
  - name: my_first_model
    config:
      materialized: view
      contract:
        enforced: true
    columns:
      - name: id
        quote: True
        data_type: varchar
        constraints:
          - type: not_null
          - type: unique

"""

_MODEL_INT_YML = """

models:
  - name: my_first_model
    config:
      materialized: view
      contract:
        enforced: true
    columns:
      - name: id
        quote: True
        data_type: bigint
        constraints:
          - type: not_null
          - type: unique

"""


class TestViewWithValidContract(BaseOBMySQLTestCase):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_first_model.sql": _MODEL_SQL,
            "my_first_model.yml": _MODEL_INT_YML,
        }

    def test_materialization_as_view_run_succeed(self, project, dbt_profile_target):
        run_dbt(args=["run"])
        adapter: BaseAdapter = project.adapter
        database = dbt_profile_target["database"]
        with adapter.connection_named("test"):
            expect = OBMySQLRelation.create(
                **{
                    "database": database,
                    "schema": database,
                    "identifier": "my_first_model",
                    "type": RelationType.View,
                }
            )
            relations: List[BaseRelation] = adapter.list_relations_without_caching(expect)
            actual = [r for r in relations if r.type == RelationType.View]
            assert [expect] == actual


class TestViewWithInValidContract(BaseOBMySQLTestCase):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_first_model.sql": _MODEL_SQL,
            "my_first_model.yml": _MODEL_CHAR_YML,
        }

    def test_materialization_as_view_exp_thrown(self, project, dbt_profile_target):
        with pytest.raises(AssertionError):
            run_dbt(args=["run"])
