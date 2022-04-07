# xls2ora

## Utility for load [xls|xlsx|csv|html] file to oracle table v.0.0.4

Usage: xls2ora.exe file.ext|file.json

file.ext [xls|xlsx|csv|html]

Example config file:

xls2ora.json =>
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

## Create table mode:
* oracle table will create as {ora_user}.tmp_{file} if {table_in} not set
* oracle columns name wiil get from row with headers

## Only load data:
* oracle columns name will get from oracle table if fields_in not set

## Common:
* cols (array) - load data only from listed numbers of columns
* truncate - delete or not data in table before load
* delete - delete with condition
* &filename - macros for replace
* required_col (array) - if data empty in this column the load will stop
* types (dict) {number of column:"float"} - for correct load float|integer|number data
