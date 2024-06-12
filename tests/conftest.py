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
import math
import os
import re
import socket
import time

import mysql.connector
import pytest

from dbt.adapters.oceanbase_mysql.connections import OBMySQLCredentials
from dbt.cli.main import dbtRunner

OB_MYSQL_TEST_HOST_KEY = "OB_MYSQL_TEST_HOST"
OB_MYSQL_TEST_PORT_KEY = "OB_MYSQL_TEST_PORT"
OB_MYSQL_TEST_USER_KEY = "OB_MYSQL_TEST_USER"
OB_MYSQL_TEST_PASSWD_KEY = "OB_MYSQL_TEST_PASSWD"
OB_MYSQL_TEST_DATABASE_KEY = "OB_MYSQL_TEST_DATABASE"

pytest_plugins = ["dbt.tests.fixtures.project"]


@pytest.fixture(scope="class")
def dbt():
    return dbtRunner()


@pytest.fixture(scope="session")
def ob_mysql_credentials() -> OBMySQLCredentials:
    kwargs = {}
    kwargs.update({"host": os.getenv(OB_MYSQL_TEST_HOST_KEY)})
    kwargs.update({"port": os.getenv(OB_MYSQL_TEST_PORT_KEY)})
    kwargs.update({"user": os.getenv(OB_MYSQL_TEST_USER_KEY)})
    kwargs.update({"password": os.getenv(OB_MYSQL_TEST_PASSWD_KEY)})
    kwargs.update({"database": os.getenv(OB_MYSQL_TEST_DATABASE_KEY)})
    for v in kwargs.values():
        assert v is not None
    database = generate_tmp_schema_name()
    with mysql.connector.connect(**kwargs) as conn:
        with conn.cursor() as cursor:
            cursor.execute("create database {};".format(database))
            cursor.fetchone()
    kwargs.update(
        {
            "schema": database,
            "database": database,
        }
    )
    yield OBMySQLCredentials.from_dict(kwargs)
    kwargs.pop("schema")
    with mysql.connector.connect(**kwargs) as conn:
        with conn.cursor() as cursor:
            cursor.execute("drop database {};".format(database))
            cursor.fetchone()


@pytest.fixture(scope="session")
def ob_mysql_connection(ob_mysql_credentials: OBMySQLCredentials):
    kwargs = {
        "host": ob_mysql_credentials.host,
        "port": ob_mysql_credentials.port,
        "user": ob_mysql_credentials.user,
        "passwd": ob_mysql_credentials.password,
        "database": ob_mysql_credentials.database,
    }
    conn = mysql.connector.connect(**kwargs)
    yield conn
    conn.close()


def generate_tmp_schema_name():
    hostname = socket.gethostname()
    hostname = re.sub(r"[\.\-\s]", "", hostname)
    length = len(hostname)
    if length > 16:
        hostname = hostname[0:8] + hostname[length - 8 : length]
    return "dbt_{0}_{1:.0f}".format(hostname, math.floor(time.time()))
