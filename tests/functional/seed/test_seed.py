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

from dbt.adapters.base import BaseAdapter
from dbt.tests.adapter.column_types.test_column_types import run_dbt
from tests.functional.utils import BaseOBMySQLTestCase

_COUNTRIES_CSV = """iso3,name,iso2,iso_numeric,cow_alpha,cow_numeric,fao_code,un_code,wb_code,imf_code,fips,geonames_name,geonames_id,r_name,aiddata_name,aiddata_code,oecd_name,oecd_code,historical_name,historical_iso3,historical_iso2,historical_iso_numeric
ABW,Aruba,AW,533,,,,533,ABW,314,AA,Aruba,3577279,ARUBA,Aruba,12,Aruba,373,,,,
AFG,Afghanistan,AF,4,AFG,700,2,4,AFG,512,AF,Afghanistan,1149361,AFGHANISTAN,Afghanistan,1,Afghanistan,625,,,,
AGO,Angola,AO,24,ANG,540,7,24,AGO,614,AO,Angola,3351879,ANGOLA,Angola,7,Angola,225,,,,
AIA,Anguilla,AI,660,,,,660,AIA,312,AV,Anguilla,3573511,ANGUILLA,Anguilla,8,Anguilla,376,,,,
ALA,Aland Islands,AX,248,,,,248,ALA,,,Aland Islands,661882,ALAND ISLANDS,,,,,,,,
ALB,Albania,AL,8,ALB,339,3,8,ALB,914,AL,Albania,783754,ALBANIA,Albania,3,Albania,71,,,,
AND,Andorra,AD,20,AND,232,6,20,ADO,,AN,Andorra,3041565,ANDORRA,,,,,,,,
ANT,Netherlands Antilles,AN,530,,,,,ANT,353,NT,Netherlands Antilles,,NETHERLANDS ANTILLES,Netherlands Antilles,211,Netherlands Antilles,361,Netherlands Antilles,ANT,AN,530
ARE,United Arab Emirates,AE,784,UAE,696,225,784,ARE,466,AE,United Arab Emirates,290557,UNITED ARAB EMIRATES,United Arab Emirates,140,United Arab Emirates,576,,,,
"""


class TestSeed(BaseOBMySQLTestCase):

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"countries.csv": _COUNTRIES_CSV}

    def test_seed_succeed(self, project):
        run_dbt(args=["seed"])
        adapter: BaseAdapter = project.adapter
        with adapter.connection_named("test"):
            _, table = adapter.execute("select * from countries", fetch=True)
            assert len(table) == 9
