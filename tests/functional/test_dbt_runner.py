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
import unittest.mock

import pytest

from dbt.cli.exceptions import DbtUsageException
from dbt.cli.main import dbtRunner


class TestDbtRunner:

    @pytest.fixture(scope="class")
    def dbt(self):
        return dbtRunner()

    def test_group_invalid_option(self, dbt: dbtRunner) -> None:
        res = dbt.invoke(["--invalid-option"])
        assert type(res.exception) == DbtUsageException

    def test_callback_method_has_been_called(self):
        mock_callback = unittest.mock.Mock()
        dbt = dbtRunner(callbacks=[mock_callback])
        dbt.invoke(args=["debug"])
        mock_callback.assert_called()
