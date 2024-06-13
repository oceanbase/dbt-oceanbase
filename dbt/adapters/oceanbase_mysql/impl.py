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
from typing import Type

import agate
from dbt_common.contracts.constraints import ConstraintType

from dbt.adapters.base import (
    BaseConnectionManager,
    BaseRelation,
    Column,
    ConstraintSupport,
)
from dbt.adapters.oceanbase_mysql.column import OBMySQLColumn
from dbt.adapters.oceanbase_mysql.connections import OBMySQLConnectionManager
from dbt.adapters.oceanbase_mysql.relation import OBMySQLRelation
from dbt.adapters.sql import SQLAdapter


class OBMySQLAdapter(SQLAdapter):

    Relation: Type[BaseRelation] = OBMySQLRelation
    Column: Type[Column] = OBMySQLColumn
    ConnectionManager: Type[BaseConnectionManager] = OBMySQLConnectionManager

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.ENFORCED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_ENFORCED,
    }

    @classmethod
    def date_function(cls):
        return "now()"

    def debug_query(self) -> None:
        self.execute("select 1 from dual")

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "timestamp"

    @classmethod
    def quote(cls, identifier: str) -> str:
        return "`{}`".format(identifier)
