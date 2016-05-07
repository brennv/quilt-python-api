# Convert files to live data sets on Quilt
## Optional prep (your steps may vary)
1. Get a list of files you want to upload (see `get-files-to-upload/`)
2. Download the files in the list (see `curl-all.py`)
3. Unzip downloaded files (if needed)
```bash
cd downloads
gunzip *.gz
```
## Upload to Quilt
4. Use `data_set.py` to create individual data sets (see `python data_set.py --help`).
You will need a Quilt username and password. Or use `batch.py` to create multiple data sets.
```bash
python data_set.py
  -e https://quiltdata.com
  -u USERNAME
  -n "ENCODE data"
  -d "#A549 #histone peak data #hg19"
  -f downloads/wgEncodeBroadHistoneNhaH3k36me3StdPk.broadPeak
```

## File formats in this example
* [ENCDOE broadPeak format](https://genome.ucsc.edu/FAQ/FAQformat.html#format13)

## Resources
* [ENCODE Project](https://www.encodeproject.org/)


# REST API
* For all REST calls, the content-type is `application/JSON`.
* Description fields automatically linkify URLs and support `<a>, <i>, <em>, <strong>, <b>` tags

*Summary*


| Action | Endpoint | Data format |
|--------|----------|-------------|
| New table | `POST /tables/` | [Format](#data-format)
| Delete table | `DELETE /tables/TABLE_ID/` |
| Update table meta-data | `PATCH /table/TABLE_ID` |
| Add column to table | `POST /tables/TABLE_ID/columns/` |
| Add row to table | `POST /data/TABLE_ID/rows/` |
| Get table rows | `GET /data/TABLE_ID/rows` |
| Get specified row | `GET /data/TABLE_ID/rows/ROW_ID` |
| Perform genome intersect or subtract | `POST /genemath/` |




## Tables
### Create new table
`POST /tables/`
#### Data format
```javascript
{
  'name': string,
  'description': text `<a>, <i>, <em>, <strong>, <b>` tags supported; automatic linkification of URLs
  'columns': [
    {
      'name': string,
      'sqlname': optional string,
      'description': optional text,
      'type' : one of 'String', 'Number', 'Image', 'Text'
    },
    ...
  ]
}
```

#### Returns
Table data as JSON object, includes `id` field with the table's identifier.

### Add column to existing table
`POST /tables/TABLE_ID/columns/`
#### Data format
```javascript
{
   'name': string,
   'sqlname': optional string,
   'description': text,
   'type': one of 'String', 'Number', 'Image', or 'Text'
}
```
#### Returns
Column data as JSON object, includes `id` field with the column's identifier.


### Delete a table
`DELETE /table/TABLE_ID`

### Update table meta-data
`PATCH /table/TABLE_ID`

#### Data format
```javascript
{
   'name': string,
   'description': text
}
```

## Table Data
* Use columns' 'sqlname' as keys in input data.

### Add rows to an existing table:
`POST /data/TABLE_ID/rows/`

#### Data format
```javascript
[
  {columnSqlname0 : value0, columnSqlname1 : value1, ... },
  ...
]
```

### Retreive Row Data
* Rows are keyed by the Quilt Row ID field 'qrid'

#### All rows
`GET /data/TABLE_ID/rows`
(NOTE: Currently limited to the first 500 rows)

#### Retrieve row by Row ID
`GET /data/TABLE_ID/rows/ROW_ID`

#### Returns
Row data as JSON object, keyed by column.sqlname.

## Genome Math
* Performs a gene math operation on two tables
* Creates a new table with the result.
* Columns are specified by column.id.

### Intersect or subtract
`POST /genemath/`

#### Data Format
```javascript
{
  'operator': one of 'Intersect' or 'Subtract',
  'left_chr': integer (column id),
  'left_start': integer (column id),
  'left_end':  integer (column id),
  'right_chr':  integer (column id),
  'right_start': integer (column id),
  'right_end':  integer (column id)
}
```
#### Returns
JSON object representing the result table.
