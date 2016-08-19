import json
import requests
import quilt

con = quilt.Connection('kmoore')
source_id = 2195

columns = [
    {'name' : 'iN_RNAseq ID',
     'sqlname' : 'in_rnaseq_id',
     'description' : 'QRID from iN_RNAseq table',
     'type' : 'Number'},
    {'name' : 'Transcript ID',
     'sqlname' : 'transcipt_id',
     'type' : 'String'}
    ]

t = con.create_table(name="Transcript IDs",
                     description="Transcript IDs separated from table %s" % source_id,
                     columns=columns)

def insert(t, i, buffer):
    response = t.create(buffer)
    if response.status_code == 200:
        print "Processed %d rows, inserting %d" % (i+1, len(buffer))
    else:
        print "Warning: Insert failed!"
        print response.text

src = con.get_table(source_id)
batch = 250
buffer = []
for i, row in enumerate(src):
    #transcript_id_s_002
    in_rna_id = int(row['qrid'])
    t_id_str = row['transcript_id_s_002']
    tids = t_id_str.split(',')
    for tid in tids:
        tuple = { 'in_rnaseq_id' : in_rna_id,
                  'transcipt_id' : tid }
        buffer.append(tuple)

    if len(buffer) >= batch:
        insert(t, i, buffer)
        buffer = []

insert(t, i, buffer)

