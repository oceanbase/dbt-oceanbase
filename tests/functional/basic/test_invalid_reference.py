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
from dbt_common.exceptions import CompilationError

from dbt.cli.main import dbtRunner
from dbt.tests.adapter.column_types.test_column_types import run_dbt
from tests.functional.utils import BaseOBMySQLTestCase

descendant_sql = """
-- should be ref('model')
select * from {{ ref(model) }}
"""


model_sql = """
select 1 as id
"""


class TestInvalidReference(BaseOBMySQLTestCase):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "descendant.sql": descendant_sql,
            "model.sql": model_sql,
        }

    def test_undefined_value(self, project, dbt: dbtRunner):
        with pytest.raises(CompilationError):
            run_dbt(args=["compile"])
