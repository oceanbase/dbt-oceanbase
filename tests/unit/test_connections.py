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
from typing import Any, Dict
from unittest import TestCase

from dbt.adapters.oceanbase_mysql.connections import OBMySQLCredentials


class TestOBMySQLCredentials(TestCase):

    def setUp(self):
        self.profile_data: Dict[str, Any] = {
            "dbname": "test",
            "schema": "test",
            "host": "127.0.0.1",
            "port": 2881,
            "user": "username@tenantname#clustername",
            "pass": "test_pass",
        }

    def test_init_credentials_from_dict_succeed(self):
        data = OBMySQLCredentials.translate_aliases(self.profile_data)
        OBMySQLCredentials.validate(data)
        expect = OBMySQLCredentials(**data)
        actual = OBMySQLCredentials.from_dict(data)
        self.assertEqual(expect, actual)
