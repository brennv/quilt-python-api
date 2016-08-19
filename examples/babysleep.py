
import json
import requests
from datetime import datetime

import quilt

con = quilt.Connection('kevin')

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
                    value = value["$date"]
    except Exception as error:
        print error
        traceback.print_exc(file=sys.stdout)
    finally:
        return value

#u'_id'
#u'babyId'
#u'breastPumpingSide' -> side
#u'breastFeedingSide' -> side
#u'quantityAmount'
#u'quantityUnits'
#u'quantityAmountLeft'
#u'quantityUnitsLeft'
#u'quantityAmountRight'
#u'quantityUnitsRight'
#u'bottleType'
#u'createdDateTime'
#u'notes'
#u'endDateTime'
#u'solidFood'
#u'diaperType'
#u'startDateTime'
#u'type'
#u'updatedDateTime'
#u'lastUpdatedByUserId'
#u'isDeleted'


columns = []
columns.append({'name' : '_id', 'type' : 'String'})
columns.append({'name' : 'babyId', 'type' : 'String'})
columns.append({'name' : 'breastFeedingSide', 'type' : 'String'})
columns.append({'name' : 'breastPumpingSide', 'type' : 'String'})
columns.append({'name' : 'quantityAmount', 'type' : 'Number'})
columns.append({'name' : 'quantityUnits', 'type' : 'String'})
columns.append({'name' : 'quantityAmountLeft', 'type' : 'Number'})
columns.append({'name' : 'quantityUnitsLeft', 'type' : 'String'})
columns.append({'name' : 'quantityAmountRight', 'type' : 'Number'})
columns.append({'name' : 'quantityUnitsRight', 'type' : 'String'})
columns.append({'name' : 'imageUrl', 'type' : 'String'})
columns.append({'name' : 'imageUrlThumb', 'type' : 'String'})
columns.append({'name' : 'imageIdentifier', 'type' : 'String'})
columns.append({'name' : 'solidFood', 'type' : 'String'})
columns.append({'name' : 'type', 'type' : 'String'})
columns.append({'name' : 'eventName', 'type' : 'String'})
columns.append({'name' : 'diaperType', 'type' : 'String'})
columns.append({'name' : 'bottleType', 'type' : 'String'})
columns.append({'name' : 'notes', 'type' : 'Text'})
columns.append({'name' : 'startDateTime', 'type' : 'DateTime'})
columns.append({'name' : 'endDateTime', 'type' : 'DateTime'})
columns.append({'name' : 'createdDateTime', 'type' : 'DateTime'})
columns.append({'name' : 'updatedDateTime', 'type' : 'DateTime'})
columns.append({'name' : 'lastUpdatedByUserId', 'type' : 'String'})
columns.append({'name' : 'isDeleted', 'type' : 'String'})
columns.append({'name' : '__v', 'type' : 'String'})

t = con.create_table(name='Baby Events', columns=columns)

fields = {c.name : c for c in t.columns}

count = 0
rowcount = 0
maxreq = 20
res = []
i = 0
with open('/Users/kmoore/Downloads/babyevents.json', 'rb') as file:
    buffer = []

    for line in file:
        count += 1
        event=json.loads(line)

        row = {}
        for k in event.keys():
            sqlname = fields[k].field
            row[sqlname] = get_field(event, k)
        buffer.append(row)
        if len(buffer) >= 250:
            rowcount += len(buffer)

            while len(res) > maxreq:
                finished = [(r, b) for r, b in res if r.ready()]
                for r, b in finished:
                    if not r.successful():
                        print "Retrying:"
                        print b
                        res.append((t.create_async(b, status_check), b))
                
                res[:] = [(r, b) for r, b in res if not r.ready()]
                if len(res) > maxreq:
                    r, b = res[0]
                    r.wait()
                
            res.append((t.create_async(buffer, status_check), buffer))
            #t.create(buffer)
            buffer = []
    t.create(buffer)
    rowcount += len(buffer)
    buffer = []


print "End of File? Line count = %d, Rows count=%s" % (count, rowcount)
    
