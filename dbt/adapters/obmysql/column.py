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
from typing import Dict

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.base import Column


class OBMySQLColumn(Column):

    _CAST_TYPE_ALIAS: Dict[str, str] = {
        "int": "signed integer",
        "bigint": "signed integer",
        "nchar": "character",
        "char": "character",
        "varchar": "character",
    }

    @property
    def quoted(self) -> str:
        return "`{}`".format(self.column)

    @classmethod
    def translate_cast_type(cls, dtype: str) -> str:
        return cls._CAST_TYPE_ALIAS.get(dtype, dtype)

    def is_integer(self) -> bool:
        return super().is_integer() or self.dtype.lower() in ["tinyint", "mediumint", "int"]

    def is_string(self) -> bool:
        return super().is_string() or self.dtype.lower() in [
            "char",
            "nchar",
            "tinytext",
            "mediumtext",
            "longtext",
        ]

    def string_size(self) -> int:
        if not self.is_string():
            raise DbtRuntimeError("Called string_size() on non-string field!")
        return 256 if self.char_size is None else int(self.char_size)
