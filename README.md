# xls2ora
xls2ora - Utility for load xls|xlsx|csv|html file to oracle table
## Summary
utility to import a sheet from a xls|xlsx|csv|html file into an Oracle Database. 
It is possible to create table, set float columns, truncate table before load data
## Home Page
https://sites.google.com/view/xls2ora/home
## Usage
xls2ora.exe file|file.json
- file - accept xls xlsx csv html file format
- file.json - config file. Can be has a different name.
### Example config file:
```json
{
"table_in":"shtat.shtat_reports",
"fields_in":"pib,time_inout,rdate",
"file_in":"file.ext",
"cols":[2,3,"&filename"],
"format":"html|xls|csv",
"truncate":"Y|n",
"delete":" rdate=\"&filename\"",
"required_col":3,
"types":{6:"float",7:"float",8:"float",9:"float",10:"float",11:"float",12:"float"},
"ora_user":"user",
"ora_pwd":"password",
"ora_dsn":"ora_dsn"
}
```

### Create table mode
* oracle table will create as {ora_user}.tmp_{file} if {table_in} not set
* oracle columns name will get from row with headers
### Only load data
* oracle columns name will get from oracle table if fields_in not set
### Common
* cols (array) - load data only from listed numbers of columns
* truncate - delete or not data in table before load
* delete - delete with condition
* &filename - macros for replace
* required_col (array) - if data empty in this column the load will stop
* types (dict) {number of column:"float"} - for correct load float|integer|number data

## People
xls2ora has been written by Viktor Holoshivskiy (vholoshivskiy@gmail.com).

# Install
```cmd
python -m venv venv 
venv/scripts/activate 
git clone https://github.com/vityah1/xls2ora 
cd xls2ora 
venv/scripts/pip3 install -r requirements.txt
```
## License
Copyright 2022 Viktor Holoshivskiy

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
