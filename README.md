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

| Action | Endpoint | Details |
|--------|----------|-------------|
| New table | `POST /tables/` | [See below](#create-table) |
| Delete table | `DELETE /tables/TABLE_ID/` | [See below](#delete-table) |
| Update table meta-data | `PATCH /tables/TABLE_ID` | [See below](#update-table-meta-data) |
| Add column to table | `POST /tables/TABLE_ID/columns/` | [See below](#add-column-to-table) |
| Append row to table | `POST /data/TABLE_ID/rows/` | [See below](#append-row) |
| Get table rows | `GET /data/TABLE_ID/rows` | [See below](#get-rows) |
| Get table row | `GET /data/TABLE_ID/rows/ROW_ID` | [See below](#get-row) |
| Genome intersect or subtract | `POST /genemath/` | [See below](#intersect-or-subtract) |

Notes
* For all REST calls, the content-type is `application/JSON`.
* Description fields automatically linkify URLs and support `<a>, <i>, <em>, <strong>, <b>` tags


## Tables
### Create table
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

### Add column to table
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


### Delete table
`DELETE /tables/TABLE_ID`

### Update table meta-data
`PATCH /tables/TABLE_ID`

#### Data format
```javascript
{
   'name': string,
   'description': text
}
```

## Table Data
* Use column `sqlname` as keys in input data

### Append row
`POST /data/TABLE_ID/rows/`

#### Data format
```javascript
[
  {columnSqlname0: value0, columnSqlname1 : value1, ... },
  ...
]
```

### Get rows
`GET /data/TABLE_ID/rows`
* Rows are keyed by the Quilt Row ID field `qrid`
* NOTE: Currently limited to the first 500 rows

#### Returns
Row data as JSON object, keyed by column.sqlname.

### Get row
`GET /data/TABLE_ID/rows/ROW_ID`

#### Returns
Row data as JSON object, keyed by column.sqlname.

## Quilt tables

### Join
`POST /quilts/`
#### Data format
```javascript
{
  'left_table_id': int,
  'right_table_id': int,
  'left_column_id': int,
  'right_column_id': int,
  'jointype': one of 'inner', 'leftOuter', 'firstMatch'
}
```

#### Returns
Quilt info as JSON object, includes `sqlname` field with the quilt's identifier.

### Undo join
`DELETE /quilts/QUILT_SQLNAME`


## Genome Math
* Performs a gene math operation on two tables
* Creates a new table with the result.
* Columns are specified by `column.id`.

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

# Python

The Quilt Python connector uses the Quilt REST API and SQL Alchemy (http://docs.sqlalchemy.org/),
if installed, to access and update data sets in Quilt. Quilt tables are available as dictionaries
or Pandas (http://pandas.pydata.org/) DataFrames.

## Connection

To use the Quilt Python connector, add this repository to your PYTHONPATH
and import quilt.

Connect to Quilt by creating a Connection object:

```python
import quilt
connection = quilt.Connection(username)
* enter your password
```

The connection will contain a list of your Quilt tables:
```python
connection.tables
```

### Search for Data Sets
You can also find tables by searching your own tables and Quilt's public data sets
connection.search('term')

## Table

Each Table object has a list of Columns
mytable.columns

After the columns have been fetched, columns are available as table attributes.
mytable.column1

### Accessing Table Data

Tables are iterable. To access table data:
for row in mytable:
    print row

#### Search
Search for matching rows in a table by calling search.

for row in mytable.search('foo'):
    print row

#### Order By
Sort the table by any column or set of columns.

mytable.order_by('column1')

mytable.order_by(mytable.column1.field)

mytable.order_by(['column2', 'column1'])

To sort in descending order, add a "-" in front of the column field name:
mytable.order_by('-column1')

#### Limit
Limit the number of rows returned by calling limit(number_of_rows).

#### Putting it all together
Search, order_by and limit can be combined to return just the data you
want to see. For example, to return the top 2 finishers with the name Sally
from a table of race results (race_results: [name_000, time_001]), you could write:

```python
for result in race_results.search('Sally').order_by('-time_001').limit(2):
    print row
```

### Pandas DataFrame

Access a table's data as a Pandas DataFrame by calling
mytable.df()

You can also combine the querying methods above to access particular rows.
race_results.search('Sally').order_by('-time_001').limit(2).df()

### Gene Math

Quilt supports intersect and subtract for tables that store genomic regions. Those
operations assume that tables have columns storing: Chromsome, start and end. The
function get_bed_cols tries to infer those columns based on column names.

If the guessing fails, or to override the guess, set the chromosome, start, end
columns explicitly with set_bed_cols.
mytable.set_bed_cols(mytable.chr_001, mytable.start_002, mytable.end_003)

Once the bed columns are set for both tables, they can be intersected and subtracted.
```python
result = tableA.intersect(tableB)
result = tableA.intersect_wao(tableB)
result = tableA.subtract(tableB)
```
