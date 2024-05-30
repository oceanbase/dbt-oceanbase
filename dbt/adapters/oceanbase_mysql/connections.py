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
from typing import Any, ContextManager, Tuple

import mysql
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
    Credentials,
)
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.sql import SQLConnectionManager

log = AdapterLogger("OceanBase")

OBMySQL_DIALECT_TYPE = "oceanbase_mysql"
OBOracle_DIALECT_TYPE = "oceanbase_oracle"


@dataclass
class OBMySQLCredentials(Credentials):

    host: str
    port: int  # [0-65535]
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
        "retry_times": "retries",
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
            "retries",
            "schema",
            "database",
            "connect_timeout_seconds",
        )


class OBMySQLConnectionManager(SQLConnectionManager):

    TYPE = OBMySQL_DIALECT_TYPE

    @contextmanager
    def exception_handler(self, sql: str) -> ContextManager:
        try:
            yield
        except Exception as e:
            log.warning("Failed to run SQL: {}".format(sql))
            log.warning("Rolling back transaction")
            try:
                self.rollback_if_open()
            except Exception:
                log.warning("Failed to rollback transaction")
            if isinstance(e, DbtRuntimeError):
                raise
            raise DbtRuntimeError(str(e)) from e

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            log.debug("Connection is already open, skip open")
            return connection

        credentials: Credentials = connection.credentials
        # the attri that mysql connector supports can be viewed at
        # https://mysql.net.cn/doc/connectors/en/connector-python-connectargs.html
        kwargs = {}
        kwargs["host"] = credentials.host
        kwargs["user"] = credentials.user
        kwargs["password"] = credentials.password
        kwargs["database"] = credentials.database
        kwargs["port"] = credentials.port
        kwargs["connection_timeout"] = credentials.connect_timeout_seconds

        conn: Connection = cls.retry_connection(
            connection,
            connect=lambda: mysql.connector.connect(**kwargs),
            logger=log,
            retry_limit=credentials.retries,
            retry_timeout=lambda p: p * p,
            retryable_exceptions=[],
        )
        sql = "select connection_id() from dual"
        with conn.handle.cursor() as cursor:
            cursor.execute(sql)
            tup = cursor.fetchone()
            assert tup is not None
            connection.connection_id = tup[0]
        return conn

    def cancel(self, connection: Connection):
        connection_id = connection.connection_id
        connection_name = connection.name
        assert connection_id is not None
        log.debug("Cancelling query '{}' ({})".format(connection_name, connection_id))
        sql = "kill query {0}".format(connection_id)
        _, cursor = self.add_query(sql)
        res = cursor.fetchone()
        log.debug("Cancel query '{}': {}".format(connection_name, res))

    @classmethod
    def get_response(cls, cursor: Any) -> AdapterResponse:
        # there is no way to get info from cursor before fetch
        code = "SUCCESS"
        rows_affected = -1
        return AdapterResponse(
            _message="{0}-{1}".format(code, rows_affected), rows_affected=rows_affected, code=code
        )
