import json
import getpass
import requests
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
        

class Table(object):
    _schema = None
    _quilts = None
    _buffer = []
    _search = None
    _ordering_fields = []
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

    def __init__(self, con, data):
        self.connection = con
        self.id = data.get('id')
        self.name = data.get('name')
        self.sqlname = data.get('sqlname')
        self.description = data.get('description')
        self.owner = data.get('owner')
        self.is_public = data.get('is_public')

        if data.has_key('columns'):
            self._schema = data.get('columns')

        if data.has_key('quilts'):
            self._quilts = data.get('quilts')            

    def __str__(self):
        return "[%04d] %s" % (self.id, self.name)

    def __eq__(self, table):
        return self.id == table.id

    def _guess_bed_columns(self):
        for c in self.columns:
            name = c['name'].lower()
            if 'chromosome' in name:
                self._chr = c['id']
            elif not self._chr and 'chr' in name:
                self._chr = c['id']

            if 'start' in name:
                self._start = c['id']
            elif 'end' in name:
                self._end = c['id']
            elif not self._end and 'stop' in name:
                self._end = c['id']                

    def delete(self):
        response = requests.delete("%s/tables/%s/" % (self.connection.url, self.id),
                                   headers=HEADERS,
                                   auth=self.connection.auth)
        if response.status_code == requests.codes.ok:
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
                self._schema = data.get('columns')
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

    def delete_column(self, column_id):
        response = requests.delete("%s/tables/%s/columns/%s/" % (self.connection.url, self.id, column_id),
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
            if data.has_key('columns'):
                self._schema = data.get('columns')
                
            if data.has_key('quilts'):
                self._quilts = [Quilt(self, d) for d in data.get('quilts')]
            else:
                self._quilts = []
        return self._quilts

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


    def order_by(self, fields):
        if not fields:
            self._ordering_fields = []
        elif isinstance(fields, list):
            self._ordering_fields = fields
        else:
            self._ordering_fields = [fields]

    def search(self, term):
        self._search = term

    def next(self):        
        
        try:
            return self._generator.next()
        except StopIteration:
            if self.nextlink:
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
                print self.nextlink
                self._buffer = []
                for row in data['results']:
                    self._buffer.append(row)
                self._generator = rowgen(self._buffer)
                return self._generator.next()
            else:
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
        res = self.connection.pool.apply_async(make_post_request,
                                               args=(url, json.dumps(data), self.connection.auth),
                                               callback=callback)
        return res

    def quilt(self, left_column, right_column):
        data = {}
        data['left_table'] = self.id
        data['left_column'] = left_column
        data['right_column'] = right_column

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
        self._chr = chr
        self._start = start
        self._end = end

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

        response = requests.get("%s/users/%s/" % (self.url, username),
                                headers=HEADERS,
                                auth=requests.auth.HTTPBasicAuth(self.username, self.password))
        self.status_code = response.status_code
        if response.status_code == requests.codes.ok:
            userdata = response.json()
            self.tables = [Table(self, d) for d in userdata['tables']]
            self.userid = userdata['id']

            self.pool = Pool(processes=8)
        else:
            print "Login Failed. Please check your credentials and try again."

    def __del__(self):
        self.pool.close()
        self.pool.join()

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

    def get_table(self, table_id):
        response = requests.get("%s/tables/%s/" % (self.url, table_id),
                                headers=HEADERS,
                                auth=self.auth)        
        
        print response.status_code
        return Table(self, response.json())

    def create_table(self, data):
        response = requests.post("%s/tables/" % self.url,
                                 data = json.dumps(data),
                                 headers=HEADERS,
                                 auth=self.auth)

        if response.status_code == requests.codes.ok:
            return Table(self, response.json())
        else:
            print response.text
            return response.text
        
    
