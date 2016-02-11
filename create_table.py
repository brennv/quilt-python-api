import os
import sys
import requests
import json
import argparse
import getpass

from requests.auth import HTTPBasicAuth

def main(argv):
    parser = argparse.ArgumentParser(description='Create a table with columns.')
    parser.add_argument('-l', '--location', default='http://localhost:8000', help='API URL.')
    parser.add_argument('-u', '--user', default=os.environ['USER'], help='Quilt user')
    parser.add_argument('-t', '--table', default='test_table', help='Table name')
    parser.add_argument('-d', '--description', default='', help='Table description')
    parser.add_argument('-c', '--column', nargs='+', action='append', help='name:type for each column')
    args = parser.parse_args(argv)
    passwd = getpass.getpass()

    h = {"Content-Type": "application/json",
         "Accept": "application/json",
         }

    table_schema = {
        "name" : args.table,
        "description" : args.description,
        "columns" : []        
    }    

    columns = [el for elements in args.column for el in elements]
    print columns
    for c in columns:
        name, type = c.split(":")
        map_entry = {"name" : name, "type" : type}
        table_schema["columns"].append(map_entry)

    print table_schema
    
    create_table_url = "%s/tables/" % (args.location)
    print create_table_url
    r = requests.post(create_table_url,
                      auth=HTTPBasicAuth(args.user, passwd),
                      data=json.dumps(table_schema),
                      headers=h)
    
    print r
    print r.text

if __name__ == "__main__":
  main(sys.argv[1:])
