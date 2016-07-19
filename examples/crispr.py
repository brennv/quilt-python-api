import requests
from datetime import datetime
import quilt

con = quilt.Connection('kmoore')

def status_check(response):
    if response:
        print response.status_code
    else:
        print "None"



ut = con.get_table(1924)
lt = con.get_table(1529)

field_map = {}
field_map['grna_name_000'] = 'grna_name_000'
field_map['gene_001'] = 'grna_targetgene_001'
field_map['grna_sequence_002'] = 'grna_sequence_002'
field_map['oligo_library_003'] = 'oligo_library_009'
field_map['oligo_plate_f_004'] = 'oligo_plate_f_007'
field_map['oligo_plate_r_005'] = 'oligo_plate_r_008'

rowcount = 0
maxreq = 20
buffer = []
res = []
for row in lt:
    nr = {}
    nr['source'] = lt.name
    for f, t in field_map.items():
        nr[f] = row[t]
    buffer.append(nr)   

    if len(buffer) >= 250:
        rowcount += len(buffer)

        while len(res) > maxreq:
            finished = [(r, b) for r, b in res if r.ready()]
            for r, b in finished:
                if not r.successful():
                    print "Retrying:"
                    print b
                    res.append((ut.create_async(b, status_check), b))

            res[:] = [(r, b) for r, b in res if not r.ready()]
            if len(res) > maxreq:
                r, b = res[0]
                r.wait()

        res.append((ut.create_async(buffer, status_check), buffer))
        #ut.create(buffer)
        buffer = []
ut.create(buffer)
rowcount += len(buffer)
buffer = []

print "Inserted = %d" % (rowcount)
    
