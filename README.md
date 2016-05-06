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

# File formats in this example
* [ENCDOE broadPeak format](https://genome.ucsc.edu/FAQ/FAQformat.html#format13)

# Resources
* [ENCODE Project](https://www.encodeproject.org/)


# Quilt REST API

## Tables
Create a table with columns:
POST /tables/
Data:
{ 'name' : <Table's Name>,
  'description' : <Markup-enabled text description of the table's contents>,
  'columns' : [            
     {'name' : <Column Name>,
      'sqlname' : (optional) API/database name for the column,
      'description' : (optional) Column description,
      'type' : <Choose from: ['String', 'Number', 'Image', 'Text']>}, ...
      ]
}
Returns table definition (e.g. as JSON). The response includes 'id', the table's identifier.

Add a column (to an existing table):
POST /tables/<Table ID>/columns/
Data:
{'name' : <Column Name>,
 'sqlname' : (optional) API/database name for the column,
 'description' : (optional) Column description,
 'type' : <Choose from: ['String', 'Number', 'Image', 'Text']>},

List/search tables accessible to current user:
GET /tables ? search=term1+term2...

Delete a table:
DELETE /table/<Table ID>

Update a table's name/description:
PATCH /table/<Table ID>
Data:
{'name' : <Table's Name>,
 'description' : <Markup-enabled text description of the table's contents>
}

## Table Data

Add rows:
POST /data/<Table ID>/rows

Data:
Field keys are the sqlname from the corresponding column.
[ { key0 : Value0, key1 : Value1, ... },
  { key0 : Value0, key1 : Value1, ...},
  ...
]

Retreive Data:
GET /data/<Table ID>/rows
(temporarily limited to 500 rows)

GET /data/<Table ID>/rows/<Row ID>

## Gene Math
Performs a gene math operation on two tables and creates a new table with the result.

POST /genemath
Columns are specified by their column id ('id').

Data:
{'operator' : <Choose from ['Intersect', 'Subtract']>,
 'left_chr' : <Chromosome column of left table>,
 'left_start' : <Start column of left table>,
 'left_end' : <End/stop column of left table>,
 'right_chr' : <Chromosome column of right table>,
 'right_start' : <Start column of right table>,
 'right_end' : <End/stop column of right table>}


 
