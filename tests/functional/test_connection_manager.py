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
import dataclasses

import pytest


from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
    HasCredentials,
    Credentials,
)
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.oceanbase_mysql.connections import (
    OBMySQLConnectionManager,
    OBMySQLCredentials,
)
from tests.functional.utils import BaseOBMySQLTestCase
from dbt.mp_context import get_mp_context


class TestOBMySQLConnectionManager(BaseOBMySQLTestCase):

    @pytest.fixture(scope="class")
    def connection_manager(self, ob_mysql_credentials: OBMySQLCredentials):
        @dataclasses.dataclass
        class MyProfile(HasCredentials):
            profile_name: str
            target_name: str
            threads: int
            credentials: Credentials

        return OBMySQLConnectionManager(
            MyProfile(None, None, 1, ob_mysql_credentials), get_mp_context()
        )

    def test_open_normal_connection_succeed(self, connection_manager: OBMySQLConnectionManager):
        try:
            conn: Connection = connection_manager.set_connection_name()
            connection_manager.execute("select 1 from dual", fetch=True)
            assert conn.state == ConnectionState.OPEN
            assert hasattr(conn, "connection_id") is True
            assert conn.connection_id is not None
        finally:
            connection_manager.release()

    def test_get_response_execute_sql_succeed(self, connection_manager: OBMySQLConnectionManager):
        try:
            connection_manager.set_connection_name()
            actual, _ = connection_manager.execute("select 1 from dual")
            code = "SUCCESS"
            rows_affected = -1
            expect = AdapterResponse(
                _message="{0}-{1}".format(code, rows_affected),
                code=code,
                rows_affected=rows_affected,
            )
            assert actual == expect
        finally:
            connection_manager.release()

    def test_rollback_if_open_tran_uncommit_succeed(
        self, connection_manager: OBMySQLConnectionManager
    ):
        try:
            connection_manager.set_connection_name()
            ddl = """
                    create table test (
                        id varchar(64)
                    )
                    """
            connection_manager.execute(ddl, fetch=True)
            connection_manager.begin()
            connection_manager.execute("insert into test values('David'), ('Marry')")
            _, table = connection_manager.execute("select * from test", fetch=True)
            assert len(table.rows) == 2
            connection_manager.rollback_if_open()
            _, table = connection_manager.execute("select * from test", fetch=True)
            assert len(table.rows) == 0
        finally:
            connection_manager.execute("drop table test")
            connection_manager.release()

    def test_rollback_if_open_tran_commit_failed(
        self, connection_manager: OBMySQLConnectionManager
    ):
        try:
            connection_manager.set_connection_name()
            ddl = """
                    create table test (
                        id varchar(64)
                    )
                    """
            connection_manager.execute(ddl, fetch=True)
            connection_manager.begin()
            connection_manager.execute("insert into test values('David'), ('Marry')")
            connection_manager.commit()
            _, table = connection_manager.execute("select * from test", fetch=True)
            assert len(table.rows) == 2
            connection_manager.rollback_if_open()
            _, table = connection_manager.execute("select * from test", fetch=True)
            assert len(table.rows) == 2
        finally:
            connection_manager.execute("drop table test")
            connection_manager.release()

    def test_exception_handler_exp_thrown(self, connection_manager: OBMySQLConnectionManager):
        sql = "select 1 fr dual"
        with pytest.raises(DbtRuntimeError):
            try:
                connection_manager.set_connection_name()
                connection_manager.execute(sql)
            finally:
                connection_manager.release()
