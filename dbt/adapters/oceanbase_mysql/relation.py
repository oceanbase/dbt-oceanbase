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
from dataclasses import dataclass, field

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.base import BaseRelation
from dbt.adapters.base.relation import SerializableIterable
from dbt.adapters.contracts.relation import Path, RelationType


@dataclass(frozen=True, repr=False, eq=False)
class OBMySQLRelation(BaseRelation):

    quote_character: str = "`"
    # the object that can be renamed,
    # e.g. rename table tbl1 to tbl2 or alter table tbl1 rename to tbl2
    # we just support table
    renameable_relations: SerializableIterable = field(
        default_factory=lambda: frozenset({RelationType.Table, RelationType.View})
    )
    # the object that can be replaced
    # e.g. create or replace xxx
    # we just support view and materialized view
    replaceable_relations: SerializableIterable = field(
        default_factory=lambda: frozenset({RelationType.View})
    )

    def replace_path(self, **kwargs):
        r = super().replace_path(**kwargs)
        self.__validate_path(r)
        return r

    def __post_init__(self):
        self.__validate_path(self.path)

    @staticmethod
    def __validate_path(path: Path):
        assert path is not None
        if path.database != path.schema:
            raise DbtRuntimeError(
                f"The schema '{path.schema}' is not equals to the database '{path.database}'"
            )

    def information_schema(self, view_name=None):
        return (
            super()
            .information_schema(view_name)
            .incorporate(path={"schema": None, "database": None})
        )

    def render(self) -> str:
        relation = self.include(schema=False)
        return ".".join(part for _, part in relation._render_iterator() if part is not None)
