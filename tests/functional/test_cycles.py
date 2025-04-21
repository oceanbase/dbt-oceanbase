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

from dbt.tests.util import run_dbt
from tests.functional.utils import BaseOBMySQLTestCase

model_a_sql = """
select * from {{ ref('model_b') }}
"""

model_b_sql = """
select * from {{ ref('model_a') }}
"""

complex_cycle__model_a_sql = """
select 1 as id
"""

complex_cycle__model_b_sql = """
select * from {{ ref('model_a') }}s
union all
select * from {{ ref('model_e') }}
"""

complex_cycle__model_c_sql = """
select * from {{ ref('model_b') }}
"""

complex_cycle__model_d_sql = """
select * from {{ ref('model_c') }}
"""

complex_cycle__model_e_sql = """
select * from {{ ref('model_e') }}
"""


class TestSimpleCycle(BaseOBMySQLTestCase):

    @pytest.fixture(scope="class")
    def models(self):
        return {"model_a.sql": model_a_sql, "model_b.sql": model_b_sql}

    def test_simple_cycle(self, project):
        with pytest.raises(RuntimeError) as exc:
            run_dbt(["run"])
        expected_msg = "Found a cycle"
        assert expected_msg in str(exc.value)


class TestComplexCycle(BaseOBMySQLTestCase):

    @pytest.fixture(scope="class")
    def models(self):
        # The cycle in this graph looks like:
        #   A -> B -> C -> D
        #        ^         |
        #        |         |
        #        +--- E <--+
        return {
            "model_a.sql": complex_cycle__model_a_sql,
            "model_b.sql": complex_cycle__model_b_sql,
            "model_c.sql": complex_cycle__model_c_sql,
            "model_d.sql": complex_cycle__model_d_sql,
            "model_e.sql": complex_cycle__model_e_sql,
        }

    def test_complex_cycle(self, project):
        with pytest.raises(RuntimeError) as exc:
            run_dbt(["run"])
        expected_msg = "Found a cycle"
        assert expected_msg in str(exc.value)
