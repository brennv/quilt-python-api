
import json
import requests
from datetime import datetime
from firebase import lazy, async

import quilt


#con = quilt.Connection('kevin', 'http://localhost:5000')
con = quilt.Connection('kmoore')

def status_check(response):
    if response:
        print response.status_code
    else:
        print "None"

def get_field(dict, key):
    value = None
    try:        
        if dict.has_key(key):
            value = dict.get(key)

            if isinstance(value, type({})):
                if value.has_key("$oid"):
                    value = value["$oid"]
                elif value.has_key("$date"):
                    ut = value["$date"]

                    try:
                        ival = int(ut)/1000
                        dt = datetime.fromtimestamp(ival)
                        value = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                    except:
                        value = None

                    if not value:
                        try:
                            ival = long(ut)/1000000000
                            dt = datetime.fromtimestamp(ival)
                            value = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                        except:
                            pass
                        
    except Exception as error:
        print error
        traceback.print_exc(file=sys.stdout)
    finally:
        return value

# babies
# lastModified
# __v
# members
# creatorId
# _id
# name


columns = []
columns.append({'name' : '_id', 'type' : 'String'})
columns.append({'name' : 'creatorId', 'type' : 'String'})
columns.append({'name' : 'name', 'type' : 'String'})
columns.append({'name' : 'members', 'type' : 'Text'})
columns.append({'name' : 'babies', 'type' : 'Text'})
columns.append({'name' : 'lastModified', 'type' : 'String'})
columns.append({'name' : '__v', 'type' : 'String'})
nests = con.create_table({'name' : 'Nests', 'columns' : columns})

#{u'bloodType': u'AB+', u'name': u'Ezra', u'dob': u'2014-06-26T01:05:05.000Z', u'gender': u'male', u'_id': {u'$oid': u'4f0697a56935d501000001d8'}, u'imgUrlSmall': u'https://s3.amazonaws.com/smallnest-prd/baby_4f0697617bf067010000014e_4f0697a56935d501000001d8_small.png'}

columns = []
columns.append({'name' : '_id', 'type' : 'String'})
columns.append({'name' : 'name', 'type' : 'String'})
columns.append({'name' : 'dob', 'type' : 'DateTime'})
columns.append({'name' : 'gender', 'type' : 'String'})
columns.append({'name' : 'bloodType', 'type' : 'String'})
columns.append({'name' : 'imgUrlSmall', 'type' : 'Text'})
babies = con.create_table({'name' : 'Babies', 'columns' : columns})

fields = {}
for c in nests.columns:
    fields[c['name']] = c

baby_fields = {}
for c in babies.columns:
    baby_fields[c['name']] = c

#all_keys = {}
count = 0
rowcount = 0
maxreq = 20
res = []
i = 0
with open('/Users/kmoore/Downloads/nests.json', 'rb') as file:
    nest_buffer = []
    babies_buffer = []

    for line in file:
        count += 1
        nest=json.loads(line)

        row = {}
        for k in nest.keys():
            sqlname = fields[k]['sqlname']
            row[sqlname] = get_field(nest, k)

        nest_babies = nest.get('babies') if nest.has_key('babies') else []
        for b in nest_babies:
            baby_row = {}
            for k in b.keys():
                sqlname = baby_fields[k]['sqlname']
                baby_row[sqlname] = get_field(b, k)
            babies_buffer.append(baby_row)
         
        nest_buffer.append(row)
        if len(nest_buffer) >= 100:
            rowcount += len(nest_buffer)

            response = nests.create(nest_buffer)
            if response.status_code != 200:
                print response.text

            response = babies.create(babies_buffer)
            if response.status_code != 200:
                for entry in babies_buffer:
                    response = babies.create(entry)
                    if response.status_code != 200:
                        print response.text
                        print entry
                    
            nest_buffer = []
            babies_buffer = []
    nests.create(nest_buffer)
    babies.create(babies_buffer)
    
    rowcount += len(nest_buffer)
    nest_buffer = []

#for k, kcount in all_keys.items():
#    print "%00d %s" % (kcount, k)

print "End of File? Line count = %d, Rows count=%s" % (count, rowcount)
    
