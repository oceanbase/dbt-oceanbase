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
import os.path
import shutil
import unittest.mock

from dbt.adapters.oceanbase_mysql import OBMySQLCredentials
from dbt.cli.main import dbtRunner, dbtRunnerResult


class TestInit:

    @unittest.mock.patch("click.confirm")
    @unittest.mock.patch("dbt.version._get_adapter_plugin_names")
    @unittest.mock.patch("click.prompt")
    def test_init_project_with_profile_existing_init_succeed(
        self,
        mock_prompt,
        mock_get_adapter,
        mock_confirm,
        dbt: dbtRunner,
        ob_mysql_credentials: OBMySQLCredentials,
    ):
        project_name = "test_init_project"
        try:
            mock_get_adapter.return_value = ["oceanbase_mysql"]
            mock_confirm.return_value = "y"
            mock_prompt.side_effect = [
                1,
                ob_mysql_credentials.host,
                ob_mysql_credentials.port,
                ob_mysql_credentials.user,
                ob_mysql_credentials.password,
                ob_mysql_credentials.database,
                1,
            ]
            actual = dbt.invoke(
                args=["init"],
                **{
                    "project_name": project_name,
                },
            )
            expect = dbtRunnerResult(
                **{
                    "success": True,
                }
            )
            assert expect == actual
        finally:
            if os.getcwd().endswith(project_name):
                shutil.rmtree(os.getcwd())
