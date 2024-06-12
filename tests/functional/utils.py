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

from dbt.adapters.oceanbase_mysql.connections import (
    OBMySQL_DIALECT_TYPE,
    OBMySQLCredentials,
)


class BaseOBMySQLTestCase:

    @pytest.fixture(scope="class")
    def unique_schema(self, ob_mysql_credentials: OBMySQLCredentials) -> str:
        return ob_mysql_credentials.schema

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, ob_mysql_credentials: OBMySQLCredentials):
        kwargs = ob_mysql_credentials.to_dict()
        for k in OBMySQLCredentials._ALIASES.keys():
            kwargs.pop(k, {})
        kwargs.update(
            {
                "type": OBMySQL_DIALECT_TYPE,
            }
        )
        return kwargs

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "example", "models": {"+materialized": "view"}}
