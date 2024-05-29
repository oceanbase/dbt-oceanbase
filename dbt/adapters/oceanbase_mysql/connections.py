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
from contextlib import contextmanager
from dataclasses import dataclass
from typing import ContextManager, Tuple, Optional

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.exceptions import DbtRuntimeError
from dbt.adapters.contracts.connection import Connection, ConnectionState, Credentials

log = AdapterLogger("OceanBase")

OBMySQL_DIALECT_TYPE = "oceanbase_mysql"
OBOracle_DIALECT_TYPE = "oceanbase_mysql"

@dataclass
class OBMySQLCredentials(Credentials):

    host: str
    port: int # [0-65535]
    user: str
    password: str
    connect_timeout_seconds = 10
    retries: int = 1

    # schema and database is confusing for developer which are concepts for postgres
    # postgresql can get multi databases and each database can get multi schemas
    # for OceanBase, there are no differences between schema and database, we treat them as the same thing
    _ALIASES = {
        "dbname": "database",
        "pass": "password",
        "passwd": "password",
        "username": "user",
        "connect_timeout": "connect_timeout_seconds",
        "retry_times": "retries"
    }

    @property
    def type(self) -> str:
        return OBMySQL_DIALECT_TYPE

    @property
    def unique_field(self) -> str:
        return self.host

    """
    just for pretty printing
    """
    def _connection_keys(self) -> Tuple[str, ...]:
        return (
            "host",
            "port",
            "user",
            "password",
            "connect_timeout_seconds",
            "retries",
            "database",
            "schema"
        )
