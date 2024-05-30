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

from dbt.adapters.contracts.connection import AdapterResponse, Connection, ConnectionState
from dbt.adapters.oceanbase_mysql.connections import (
    OBMySQL_DIALECT_TYPE,
    OBMySQLConnectionManager,
    OBMySQLCredentials,
)


class TestOBMySQLConnectionManager:

    @pytest.fixture(scope="class")
    def connection(self, ob_mysql_credentials: OBMySQLCredentials):
        return Connection(OBMySQL_DIALECT_TYPE, None, ob_mysql_credentials)

    def test_open_normal_connection_succeed(self, connection: Connection):
        conn: Connection = OBMySQLConnectionManager.open(connection)
        assert conn.state == ConnectionState.OPEN
        assert hasattr(conn, "connection_id") is True
        assert conn.connection_id is not None
        OBMySQLConnectionManager.close(conn)

    def test_get_response_execute_sql_succeed(self, ob_mysql_connection):
        with ob_mysql_connection.cursor() as cursor:
            cursor.execute("select 1 from dual")
            actual: AdapterResponse = OBMySQLConnectionManager.get_response(cursor)
            cursor.fetchone()
            code = "SUCCESS"
            rows_affected = -1
            expect = AdapterResponse(
                _message="{0}-{1}".format(code, rows_affected),
                rows_affected=rows_affected,
                code=code,
            )
            assert actual == expect
