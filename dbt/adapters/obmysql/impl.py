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
import itertools
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type

import agate
from dbt_common.contracts.constraints import ColumnLevelConstraint, ConstraintType
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.exceptions import DbtRuntimeError, DbtValidationError

from dbt.adapters.base import BaseConnectionManager, BaseRelation
from dbt.adapters.base import Column as BaseColumn
from dbt.adapters.base import ConstraintSupport, available
from dbt.adapters.obmysql.column import OBMySQLColumn
from dbt.adapters.obmysql.connections import OBMySQLConnectionManager
from dbt.adapters.obmysql.relation import OBMySQLRelation
from dbt.adapters.relation_configs import RelationConfigChangeAction
from dbt.adapters.sql import SQLAdapter


@dataclass
class OBMySQLIndex(dbtClassMixin):

    columns: List[str]
    algorithm: Optional[str] = field(default=None, compare=False)
    unique: Optional[bool] = None
    name: Optional[str] = field(default=None)
    options: Optional[List[str]] = field(default=None, compare=False)
    column_groups: Optional[List[str]] = field(default=None, compare=False)

    def get_name(self, relation: BaseRelation):
        if self.name is not None:
            return self.name
        if self.columns is None:
            raise DbtRuntimeError("Index columns can not be empty")
        return "dbt_idx_{0}_{1}".format(
            relation.quote(identifier=False).include(database=False, schema=False),
            "_".join([name for name in self.columns]),
        )


class OBMySQLAdapter(SQLAdapter):

    Relation: Type[BaseRelation] = OBMySQLRelation
    Column: Type[BaseColumn] = OBMySQLColumn
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

    def get_column_schema_from_query(self, sql: str) -> List[BaseColumn]:
        _, cursor = self.connections.add_select_query(sql)
        cursor.fetchone()
        return [
            self.Column.create(
                column_name, self.connections.data_type_code_to_name(column_type_code)
            )
            for column_name, column_type_code, *_ in cursor.description
        ]

    @classmethod
    def render_column_constraint(cls, constraint: ColumnLevelConstraint) -> Optional[str]:
        constraint_expression = constraint.expression or ""
        if constraint.type == ConstraintType.unique:
            rendered_column_constraint = f"unique key {constraint_expression}"
        elif constraint.type == ConstraintType.foreign_key:
            raise DbtRuntimeError("Foreign references should not appear in a column")
        else:
            return super().render_column_constraint(constraint)
        if rendered_column_constraint:
            rendered_column_constraint = rendered_column_constraint.strip()
        return rendered_column_constraint

    @available
    def parse_index(self, raw_index: Dict[str, Any]) -> OBMySQLIndex:
        try:
            OBMySQLIndex.validate(raw_index)
            return OBMySQLIndex.from_dict(raw_index)
        except Exception:
            raise DbtValidationError(f"Could not parse constraint: {raw_index}")

    @available
    def translate_cast_type(self, dtype: str) -> str:
        return OBMySQLColumn.translate_cast_type(dtype)

    @available
    def list_indexes(self, relation: BaseRelation) -> List[OBMySQLIndex]:
        results = self.execute_macro("obmysql__list_indexes", kwargs={"relation": relation})
        relations = []
        for idx_name, items in itertools.groupby(results, lambda item: item["Key_name"]):
            kwargs = {"name": idx_name}
            columns = []
            for item in items:
                columns.append(item["Column_name"])
                kwargs.update(
                    {
                        "algorithm": item["Index_type"],
                        "unique": int(item["Non_unique"]) == 0,
                    }
                )
            kwargs.update({"columns": columns})
            relations.append(OBMySQLIndex.from_dict(kwargs))
        return relations

    @available
    def compare_indexes(
        self, src: List[OBMySQLIndex], dest: List[OBMySQLIndex]
    ) -> List[Dict[str, OBMySQLIndex]]:
        return [
            {"action": RelationConfigChangeAction.drop, "context": idx}
            for idx in src
            if idx not in dest
        ] + [
            {"action": RelationConfigChangeAction.create, "context": idx}
            for idx in dest
            if idx not in src
        ]

    def valid_incremental_strategies(self):
        return ["append", "delete+insert"]

    @classmethod
    def convert_number_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "float"

    def run_sql_for_tests(self, sql, fetch, conn):
        cursor = conn.handle.cursor()
        try:
            return_val = None
            last_cursor = None
            for item in cursor.execute(sql, multi=True):
                last_cursor = item
            if fetch == "one":
                return_val = last_cursor.fetchone()
            elif fetch == "all":
                return_val = last_cursor.fetchall()
            conn.handle.commit()
            return return_val
        except BaseException:
            if conn.handle and not getattr(conn.handle, "closed", True):
                conn.handle.rollback()
            raise
        finally:
            conn.transaction_open = False

    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = "hour") -> str:
        return f"{add_to} + interval {number} {interval}"

    def string_add_sql(self, add_to: str, value: str, location="append") -> str:
        if location == "append":
            return f"concat({add_to}, '{value}')"
        elif location == "prepend":
            return f"concat('{value}', {add_to})"
        else:
            raise DbtRuntimeError(f'Got an unexpected location value of "{location}"')
