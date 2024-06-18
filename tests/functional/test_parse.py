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

from dbt.cli.main import dbtRunner, dbtRunnerResult
from tests.functional.utils import BaseOBMySQLTestCase


class TestParse(BaseOBMySQLTestCase):

    def test_no_content_parse_succeed(self, project, dbt: dbtRunner):
        res: dbtRunnerResult = dbt.invoke(args=["parse"])
        assert res.success == True


class TestParseWithModels(BaseOBMySQLTestCase):

    __MODEL_SQL = """
    select 1 from dual
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": self.__MODEL_SQL,
            "model_two.sql": self.__MODEL_SQL,
            "model_three.sql": self.__MODEL_SQL,
        }

    def test_three_models_parse_succeed(self, project, dbt: dbtRunner, models):
        res: dbtRunnerResult = dbt.invoke(args=["parse"])
        assert res.success == True
        assert len(res.result.nodes) == len(models)
