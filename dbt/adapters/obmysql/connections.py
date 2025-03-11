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
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, ContextManager, Dict, Optional, Tuple, Union

import mysql.connector
from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.utils import cast_to_str

from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
    Credentials,
)
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events.types import ConnectionUsed, SQLQuery, SQLQueryStatus
from dbt.adapters.sql import SQLConnectionManager

log = AdapterLogger("OceanBase")

OBMySQL_DIALECT_TYPE = "obmysql"
OBOracle_DIALECT_TYPE = "oboracle"


@dataclass
class OBMySQLCredentials(Credentials):

    host: str
    port: int  # [0-65535]
    user: str
    password: str
    connect_timeout_seconds = 10
    retries: int = 1
    ssl_ca: str = ""

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
        "sslca": "ssl_ca",
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
            "ssl_ca",
        )

    def __post_init__(self):
        if self.schema is None and self.database is None:
            raise DbtRuntimeError("The schema and database can not be both null")
        if self.schema is None:
            self.schema = self.database
        elif self.database is None:
            self.database = self.schema

    @classmethod
    def translate_aliases(cls, kwargs: Dict[str, Any], recurse: bool = False) -> Dict[str, Any]:
        data = super().translate_aliases(kwargs, recurse)
        if "schema" not in data:
            data.update({"schema": data.get("database", None)})
        elif "database" not in data:
            data.update({"database": data.get("schema", None)})
        return data


class OBMySQLConnectionManager(SQLConnectionManager):

    TYPE = OBMySQL_DIALECT_TYPE

    @contextmanager
    def exception_handler(self, sql: str) -> ContextManager:
        try:
            yield
        except Exception as e:
            log.warning("Failed to run SQL: {}".format(sql))
            try:
                self.rollback_if_open()
                log.warning("Roll back transaction succeed")
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
        if credentials.ssl_ca and credentials.ssl_ca.strip():
            kwargs["ssl_ca"] = credentials.ssl_ca

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

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> Tuple[Connection, Any]:
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()
        fire_event(
            ConnectionUsed(
                conn_type=self.TYPE,
                conn_name=cast_to_str(connection.name),
                node_info=get_node_info(),
            )
        )
        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = "{}...".format(sql[:512])
            else:
                log_sql = sql
            fire_event(
                SQLQuery(
                    conn_name=cast_to_str(connection.name),
                    sql=log_sql,
                    node_info=get_node_info(),
                )
            )
            pre = time.time()
            cursor = connection.handle.cursor()
            for item in cursor.execute(sql, bindings, multi=True):
                last_cursor = item
            fire_event(
                SQLQueryStatus(
                    status=str(self.get_response(cursor)),
                    elapsed=round((time.time() - pre)),
                    node_info=get_node_info(),
                )
            )
            return connection, last_cursor

    @classmethod
    def get_response(cls, cursor: Any) -> AdapterResponse:
        # there is no way to get info from cursor before fetch
        code = "SUCCESS"
        rows_affected = -1
        return AdapterResponse(
            _message="{0}-{1}".format(code, rows_affected), rows_affected=rows_affected, code=code
        )

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[int, str]) -> str:
        field_type_values = mysql.connector.constants.FieldType.desc.values()
        mapping = {code: name for code, name in field_type_values}
        return mapping[type_code]
