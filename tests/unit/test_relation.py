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
from unittest import TestCase

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.contracts.relation import Path
from dbt.adapters.exceptions import ApproximateMatchError
from dbt.adapters.oceanbase_mysql.relation import OBMySQLRelation


class TestOBMySQLRelation(TestCase):

    def setUp(self):
        self.path = Path(
            **{
                "database": "dbname",
                "schema": "dbname",
                "identifier": "tbl",
            }
        )

    def test_matches_same_object_equals(self):
        r = OBMySQLRelation(path=self.path)
        self.assertTrue(r.matches(database="dbname", schema="dbname", identifier="tbl"))

    def test_matches_different_object_not_equals(self):
        r = OBMySQLRelation(
            path=self.path.replace(
                **{
                    "identifier": "`tbl`",
                }
            )
        )
        self.assertFalse(r.matches(database="dbname", schema="dbname", identifier="tbl1"))

    @pytest.mark.xfail(raises=ApproximateMatchError)
    def test_matches_same_object_not_equals(self):
        r = OBMySQLRelation(path=self.path)
        self.assertFalse(r.matches(database="dbname", schema="dbname", identifier="`tbl`"))

    @pytest.mark.xfail(raises=DbtRuntimeError)
    def test_schema_database_not_equals_exp_thrown(self):
        OBMySQLRelation(
            path=Path(
                **{
                    "schema": "b",
                    "database": "a",
                }
            )
        )

    @pytest.mark.xfail(raises=DbtRuntimeError)
    def test_replace_schema_database_not_equals_exp_thrown(self):
        OBMySQLRelation(path=self.path).replace_path(schema="new_tbl")

    def test_render_exclude_identifier_exclude_succeed(self):
        r = OBMySQLRelation(self.path)
        self.assertEqual("`dbname`.`tbl`", r.render())
        r = r.include(identifier=False)
        self.assertEqual("`dbname`", r.render())

    def test_render_exclude_quote_identifier_exclude_succeed(self):
        r = OBMySQLRelation(self.path)
        self.assertEqual("`dbname`.`tbl`", r.render())
        r = r.quote(identifier=False)
        self.assertEqual("`dbname`.tbl", r.render())
