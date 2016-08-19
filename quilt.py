import json
import getpass
import requests
import sys

try:
    import pandas
    PANDAS = True
except:
    PANDAS = False

try:
    import psycopg2
    import sqlalchemy
    SQLALCHEMY = True
except:
    SQLALCHEMY = False

from mimetypes import MimeTypes
from multiprocessing import Pool

HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}
QUILT_URL = 'https://quiltdata.com'

def make_post_request(url, data, auth):
    response = None
    try:
        response = requests.post(url,
                                 data=data,
                                 headers=HEADERS,
                                 auth=auth,
                                 timeout=30)
    except Exception as error:
        print error
        traceback.print_exc(file=sys.stdout)
    finally:
        return response

def rowgen(buffer):
    for row in buffer:
        yield row

class File(object):
    def __init__(self, connection, data):
        self._data = data
        self.connection = connection
        self.id = data['id']
        self.owner = data['owner']
        self.filename = data['filename']
        self.fullpath = data['fullpath']
        self.url = data['url']
        self.creds = data['s3creds']
        self.upload_url = data['upload_url']
        self.status = data['status']

    def refresh(self):
        response = requests.get("%s/files/%s/" % (self.connection.url, self.id),
                                headers=HEADERS,
                                auth=self.connection.auth)
        if response.status_code == requests.codes.ok:
            self.__init__(self.connection, response.json())
        else:
            print "Oops, something went wrong."
            print response.status_code
        return response

    def download(self):
        url = self.url
        outfile = self.filename

        r = requests.get(url, stream=True)
        with open(outfile, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024): 
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
        return outfile
        

class Quilt(object):
    def __init__(self, table, data):
        self.table = table
        self.id = data['sqlname']
        self.left_column = data['left_column']
        self.right_column = data['right_column']
        self.jointype = data['jointype']
        self.right_table = data['right_table_name']

    def delete(self):
        if not self.id:
            return requests.codes.not_found
        
        connection = self.table.connection
        response = requests.delete("%s/quilts/%s/" % (connection.url, self.id),
                                   headers=HEADERS,
                                   auth=connection.auth)
        if response.status_code == requests.codes.no_content:
            self.table._quilts = None
            self.id = None
        return response.status_code


class Column(object):    
    def __init__(self, table, id):
        self.table = table
        self.id = id

    def __init__(self, table, data):
        self.table = table
        self.id = data['id']
        self.name = data['name']
        self.description = data['description']
        self.type = data['type']
        self.field = data['sqlname']

    def __str__(self):
        return "%s-%s (%s)" % (self.type, self.field, self.name)

    def __repr__(self):
        return "<quilt.Column %s.%s>" % (self.table.sqlname, self.field)

class Table(object):
    _schema = None
    _quilts = None
    _chr = None
    _start = None
    _end = None
    
    def __init__(self, con, id, name, sqlname, description, owner, is_public):
        self.nextlink = None
        self.connection = con
        self.id = id
        self.name = name
        self.sqlname = sqlname
        self.description = description
        self.owner = owner
        self.is_public = is_public
        self._reset_iteration()

    def __init__(self, con, data):
        self.connection = con
        self.id = data.get('id')
        self.name = data.get('name')
        self.sqlname = data.get('sqlname')
        self.description = data.get('description')
        self.owner = data.get('owner')
        self.is_public = data.get('is_public')
        self._reset_iteration()
        
        if data.has_key('columns'):
            self._schema = [Column(self, cdata) for cdata in data.get('columns')]
            
        if data.has_key('quilts'):
            self._quilts = data.get('quilts')

    def __str__(self):
        return "[%04d] %s" % (self.id, self.name)

    def __repr__(self):
        return "<quilt.Table %d.%s>" % (self.id, self.sqlname)

    def __eq__(self, table):
        return self.id == table.id

    def _guess_bed_columns(self):
        for c in self.columns:
            name = c.name.lower()
            if 'chromosome' in name:
                self._chr = c
            elif not self._chr and 'chr' in name:
                self._chr = c

            if 'start' in name and not self._start:
                self._start = c
            elif 'end' in name and not self._end:
                self._end = c
            elif not self._end and 'stop' in name:
                self._end = c

    def delete(self):
        response = requests.delete("%s/tables/%s/" % (self.connection.url, self.id),
                                   headers=HEADERS,
                                   auth=self.connection.auth)
        if response.status_code == requests.codes.no_content:
            self.id = None
            self.name = None
            self.description = None
            self.owner = None
            self.sqlname = None
            self._schema = None
            self._quilts = None
        else:
            print "Oops, something went wrong."
            print response.text

    @property
    def columns(self):
        if not self._schema:
            response = requests.get("%s/tables/%s/" % (self.connection.url, self.id),
                                    headers=HEADERS,
                                    auth=self.connection.auth)
            data = response.json()
            if data.has_key('columns'):
                self._schema = [Column(self, cdata) for cdata in data.get('columns')]

        for c in self._schema:
            setattr(self, c.field, c)

        return self._schema    

    def add_column(self, name, type, sqlname=None, description=None):
        data = { 'name' : name,
                 'type' : type}
        if sqlname:
            data['sqlname'] = sqlname

        if description:
            data['description'] = description
                 
        response = requests.post("%s/tables/%s/columns/" % (self.connection.url, self.id),
                                 headers=HEADERS,
                                 data=json.dumps(data),
                                 auth=self.connection.auth)
        if response.status_code == requests.codes.ok:
            newcol = response.json()
            self._schema = None
            return newcol
        else:
            print "Oops, something went wrong"
            print response.text
            return None

    def delete_column(self, column):
        response = requests.delete("%s/tables/%s/columns/%s/" % (self.connection.url, self.id, column.id),
                                   headers=HEADERS,
                                   auth=self.connection.auth)
        if response.status_code == requests.codes.no_content:            
            self._schema = None
            return None
        else:
            print "Oops, something went wrong"
            print response.text
            return None
        
    @property
    def quilts(self):
        if not self._quilts is None:
            response = requests.get("%s/tables/%s/" % (self.connection.url, self.id),
                                    headers=HEADERS,
                                    auth=self.connection.auth)
            data = response.json()
            if data.has_key('quilts'):
                self._quilts = [Quilt(self, d) for d in data.get('quilts')]
            else:
                self._quilts = []
        return self._quilts

    def df(self):
        if not PANDAS:
            print "Install pandas to use DataFrames: http://pandas.pydata.org/"
            return None        

        if self.connection._sqlengine and self._search is None:            
            type_map = {
                'String' : sqlalchemy.String,
                'Number' : sqlalchemy.Float,
                'Text' : sqlalchemy.Text,
                'Date' : sqlalchemy.Date,
                'DateTime' : sqlalchemy.DateTime,
                'Image' : sqlalchemy.String }
            
            columns = [sqlalchemy.Column(c.field, type_map[c.type]) for c in self.columns]
            table = sqlalchemy.Table(self.sqlname, sqlalchemy.MetaData(),*columns)

            stmt = sqlalchemy.select([table])
            if self._ordering_fields:
                ordering_clause = []
                for f in self._ordering_fields:
                    if f.startswith("-"):
                        fname = f.lstrip("-")
                        ordering_clause.append(getattr(table.c, fname).desc())
                    else:
                        ordering_clause.append(getattr(table.c, f).asc())
                stmt = stmt.order_by(*ordering_clause)

            if self._limit is not None:
                stmt = stmt.limit(self._limit)
            
            return pandas.read_sql(stmt, self.connection._sqlengine)
        else:
            data = []
            index = []
            columns = [c['sqlname'] for c in self.columns]
            for i, row in enumerate(self):
                if limit and i>limit:
                    break
                index.append(row['qrid'])
                data.append(row)
            return pandas.DataFrame(data, columns=columns, index=index)

    def __getitem__(self, qrid):
        response = requests.get("%s/data/%s/rows/%s" % (self.connection.url, self.id, qrid),
                                headers=HEADERS,
                                auth=self.connection.auth)
        return response.json()

    def __delitem__(self, qrid):
        response = requests.delete("%s/data/%s/rows/%s" % (self.connection.url, self.id, qrid),
                                   headers=HEADERS,
                                   auth=self.connection.auth)
        return response.status_code

    def __iter__(self):
        self._buffer = []
        self._generator = rowgen(self._buffer)
        self.nextlink = "%s/data/%s/rows/" % (self.connection.url, self.id)
        return self

    def _genemath(self, b, operator):
        a_chr, a_start, a_end = self.get_bed_cols()
        if not (a_chr and a_start and a_end):
            print "Chromosome, start, stop columns not found."
            return
    
        b_chr, b_start, b_end = b.get_bed_cols()
        if not (b_chr and b_start and b_end):
            print "Chromosome, start, stop columns not found in table %s." % b.name
            return

        data = { 'left_chr' : a_chr,
                 'left_start' : a_start,
                 'left_end' : a_end,
                 'right_chr' : b_chr,
                 'right_start' : b_start,
                 'right_end' : b_end,
                 'operator' : operator }
        response = requests.post("%s/genemath/" % self.connection.url,
                                 data = json.dumps(data),
                                 headers=HEADERS,
                                 auth=self.connection.auth)
        return response

    def export(self):
        response = requests.get("%s/data/%s/rows/export" % (self.connection.url, self.id),
                                headers=HEADERS,
                                auth=self.connection.auth)        
        return File(self.connection, response.json())

    def order_by(self, fields):
        if not fields:
            self._ordering_fields = []
        elif isinstance(fields, list):
            self._ordering_fields = fields
        else:
            self._ordering_fields = [fields]
        return self.__iter__()

    def search(self, term):
        self._search = term
        return self.__iter__()

    def limit(self, limit):
        self._limit = limit
        return self

    def _reset_iteration(self):
        self._buffer = []
        self._limit = None
        self._count = 0
        self._search = None
        self._ordering_fields = []

    def __at_limit(self):
        return (self._limit is not None and self._count >= self._limit)

    def __nextrow(self):        
        row = self._generator.next()
        self._count += 1
        return row
        
    def next(self):
        if self.__at_limit():
            self._reset_iteration()
            raise StopIteration()

        try:
            return self.__nextrow()
        except StopIteration:
            if self.nextlink:
                assert not self.__at_limit(), "Already checked for limit"
                params = {}
                if self._ordering_fields:
                    params['ordering'] = [f for f in self._ordering_fields]

                if self._search:
                    params['search'] = self._search
                    
                response = requests.get(self.nextlink,
                                        headers=HEADERS,
                                        params=params,
                                        auth=self.connection.auth)
                data = response.json()
                self.nextlink = data['next']
                self._buffer = []
                for row in data['results']:
                    self._buffer.append(row)                    
                self._generator = rowgen(self._buffer)
                return self.__nextrow()
            else:
                self._reset_iteration()
                raise StopIteration()

    def create(self, data):
        response = requests.post("%s/data/%s/rows/" % (self.connection.url, self.id),
                                 data = json.dumps(data),
                                 headers=HEADERS,
                                 auth=self.connection.auth)

        return response

    def create_async(self, data, callback=None):
        """
        Use an asynchronous POST request with the process pool.
        """
        url = "%s/data/%s/rows/" % (self.connection.url, self.id)
        res = self.connection.get_thread_pool().apply_async(make_post_request,
                                                            args=(url, json.dumps(data), self.connection.auth),
                                                            callback=callback)
        return res

    def quilt(self, left_column, right_column):
        data = {}
        data['left_table'] = self.id
        data['left_column'] = left_column.id
        data['right_column'] = right_column.id

        response = requests.post("%s/quilts/" % self.connection.url,
                                 data = json.dumps(data),
                                 headers=HEADERS,
                                 auth=self.connection.auth)
        if response.status_code == requests.codes.ok:
            i = self.__iter__() # reset iterator
            data=response.json()
            q = Quilt(self, data)
            if self.quilts is not None:
                self.quilts.append(q)
            return q
        else:
            print "Oops, something went wrong."
            print "response=%s" % response.status_code
            return None

    def set_bed_cols(self, chr, start, end):
        self._chr = chr.id
        self._start = start.id
        self._end = end.id

    def get_bed_cols(self):
        if not (self._chr and self._start and self._end):
            self._guess_bed_columns()
        return self._chr, self._start, self._end

    def intersect(self, b):
        return self._genemath(b, 'Intersect')

    def subtract(self, b):
        return self._genemath(b, 'Subtract')

    def intersect_wao(self, b):
        return self._genemath(b, 'Intersect_WAO')

    def intersect(self, b):
        a_chr, a_start, a_end = self.get_bed_cols()
        if not (a_chr and a_start and a_end):
            print "Chromosome, start, stop columns not found."
            return
    
        b_chr, b_start, b_end = b.get_bed_cols()
        if not (b_chr and b_start and b_end):
            print "Chromosome, start, stop columns not found in table %s." % b.name
            return

        data = { 'left_chr' : a_chr,
                 'left_start' : a_start,
                 'left_end' : a_end,
                 'right_chr' : b_chr,
                 'right_start' : b_start,
                 'right_end' : b_end,
                 'operator' : 'Intersect' }
        response = requests.post("%s/genemath/" % self.connection.url,
                                 data = json.dumps(data),
                                 headers=HEADERS,
                                 auth=self.connection.auth)
        return response


class Connection(object):
    
    def __init__(self, username, url=QUILT_URL):
        self.url = url
        self.username = username
        self.password = getpass.getpass()
        self.auth = requests.auth.HTTPBasicAuth(self.username, self.password)
        self.status_code = None
        self.userid = None
        self._tables = None
        self._files = None
        self._pool = None
        self._sqlengine = None
        response = requests.get("%s/users/%s/" % (self.url, username),
                                headers=HEADERS,
                                auth=requests.auth.HTTPBasicAuth(self.username, self.password))
        self.status_code = response.status_code
        if response.status_code == requests.codes.ok:
            userdata = response.json()
            self._tables = [Table(self, d) for d in userdata['tables']]
            self.userid = userdata['id']
            self.profile = userdata['profile']
            if SQLALCHEMY:
                self._sqlengine = sqlalchemy.create_engine(self.profile.get('odbc').get('url'))
        else:
            print "Login Failed. Please check your credentials and try again."

    def __del__(self):
        if self._pool:
            self._pool.close()
            self._pool.join()

    def get_thread_pool(self):
        if not self._pool:
            self._pool = Pool(processes=8)
        return self._pool

    def search(self, search):
        matches = []
        if isinstance(search, list):
            terms = search
        else:
            terms = [search]

        params = {'search' : terms}
        response = requests.get("%s/tables/" % (self.url),
                                headers=HEADERS,
                                params=params,
                                auth=self.auth)
        if response.status_code == 200:
            data = response.json()
            matches = [Table(self, d) for d in data]
        else:
            print "Oops, something went wrong."
            print "response=%s" % response.status_code

        return matches        

    @property
    def tables(self):
        if not self._tables:
            response = requests.get("%s/users/%s/" % (self.url, self.username),
                                    headers=HEADERS,
                                    auth=requests.auth.HTTPBasicAuth(self.username, self.password))
            self.status_code = response.status_code
            if response.status_code == requests.codes.ok:
                userdata = response.json()
                self._tables = [Table(self, d) for d in userdata['tables']]
            else:
                print "Oops, something went wrong."
                print "response=%s" % response.status_code
                self._tables = []
        return self._tables

    @property
    def files(self):
        if not self._files:
            response = requests.get("%s/files/" % (self.url),
                                    headers=HEADERS,
                                    auth=requests.auth.HTTPBasicAuth(self.username, self.password))
            self.status_code = response.status_code
            if response.status_code == requests.codes.ok:
                filedata = response.json()
                print filedata
                self._filedata = filedata
                self._files = [File(self, d) for d in filedata['results']]
            else:
                print "Oops, something went wrong."
                print "response=%s" % response.status_code
                self._files = []
        return self._files

    def get_table(self, table_id):
        response = requests.get("%s/tables/%s/" % (self.url, table_id),
                                headers=HEADERS,
                                auth=self.auth)        
        
        if response.status_code == requests.codes.ok:
            return Table(self, response.json())
        else:
            print "Oops, something went wrong."
            print response.text
            return None

    def create_table(self, name, description=None, columns=None, inputfile=None):
        data = { 'name' : name }
        if description:
            data['description'] = description
        if inputfile:
            if columns:
                print "Please specify either a set of columns or an input file, not both"
                return None
                
            if isinstance(inputfile, File):
                data['csvfile'] = inputfile.fullpath
            else:
                f = self.upload(inputfile)
                data['csvfile'] = f.fullpath
        elif columns:
            data['columns'] = columns
            
        response = requests.post("%s/tables/" % self.url,
                                 data = json.dumps(data),
                                 headers=HEADERS,
                                 auth=self.auth)

        if response.status_code == requests.codes.ok:
            return Table(self, response.json())
        else:
            print response.text
            return response.text

    def save_df(self, df, name, description=None):
        type_map = { 'object' : 'String',
                     'float16' : 'Number',
                     'float32' : 'Number',
                     'float64' : 'Number',
                     'int8' : 'Number',
                     'int16' : 'Number',
                     'int32' : 'Number',
                     'int64' : 'Number',
                     'unicode' : 'String' }
        
        if not PANDAS:
            print "Install pandas to use DataFrames: http://pandas.pydata.org/"
            return None

        schema = { 'name' : name, 'columns' : [] }
        if description:
            schema['description'] = description
            
        for i, col in enumerate(df.columns):
            dt = df.dtypes[i]
            ctype = type_map.get(str(dt), None)
            if not ctype:
                print "Oops, unrecognized type %s in Data Frame" % dt
                return None
            
            schema['columns'].append({'name' : col, 'type' : ctype }) 

        response = requests.post("%s/tables/" % self.url,
                                 data = json.dumps(schema),
                                 headers=HEADERS,
                                 auth=self.auth)

        if response.status_code == requests.codes.ok:
            table = Table(self, response.json())
        else:
            print response.text
            return None

        response = table.create(df.to_dict('records').values())
        if response.status_code != requests.codes.ok:
            print "Oops, something went wrong."
            print response.text

        return table

    def upload(self, filepath):
        filename = filepath.split('/')[-1]
        mime = MimeTypes()
        mime_type = mime.guess_type(filename)
        data = { 'filename' : filename, 'mime_type' : mime_type }
        response = requests.post("%s/files/" % self.url,
                                 data = json.dumps(data),
                                 headers=HEADERS,
                                 auth=self.auth)

        if response.status_code == requests.codes.created:
            f = File(self, response.json())
            with open(filepath, 'rb') as localfile:
                response = requests.put(f.upload_url,
                                        data=localfile)
                return f
        else:
            print response.text


    
