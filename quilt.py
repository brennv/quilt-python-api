import getpass
import requests

HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}
QUILT_URL = 'https://quiltdata.com'

def rowgen(buffer):
    for row in buffer:
        yield row

class Table(object):
    _schema = {}
    _buffer = []
    
    def __init__(self, auth, id, name, sqlname, description, owner, is_public):
        self.nextlink = None
        self.auth = auth
        self.id = id
        self.name = name
        self.sqlname = sqlname
        self.description = description
        self.owner = owner
        self.is_public = is_public

    def __init__(self, auth, data):
        self.auth = auth
        self.id = data.get('id')
        self.name = data.get('name')
        self.sqlname = data.get('sqlname')
        self.description = data.get('description')
        self.owner = data.get('owner')
        self.is_public = data.get('is_public')

        if data.has_key('columns'):
            self._schema = data.get('columns')

    @property
    def columns(self):
        if not self._schema:
            response = requests.get("%s/tables/%s/" % (QUILT_URL, self.id),
                                    headers=HEADERS,
                                    auth=self.auth)
            data = response.json()
            if data.has_key('columns'):
                self._schema = data.get('columns')
        return self._schema

    def __iter__(self):
        self._buffer = []
        self._generator = rowgen(self._buffer)
        self.nextlink = "%s/data/%s/rows/" % (QUILT_URL, self.id)
        return self

    def next(self):        
        
        try:
            return self._generator.next()
        except StopIteration:
            if self.nextlink:
                response = requests.get(self.nextlink,
                                        headers=HEADERS,
                                        auth=self.auth)
                data = response.json()
                self.nextlink = data['next']
                print self.nextlink
                for row in data['results']:
                    self._buffer.append(row)
                self._generator = rowgen(self._buffer)
                return self._generator.next()
            else:
                raise StopIteration()        


# Filter this by user
# Use getpass to enter 
class Connection(object):
    
    def __init__(self, username):
        self.username = username
        self.password = getpass.getpass()
        self.auth = requests.auth.HTTPBasicAuth(self.username, self.password)

        #response = requests.get("%s/users/%s/" % (QUILT_URL, username),
        #                       headers=HEADERS,
        #                       auth=requests.auth.HTTPBasicAuth(self.username, self.password))        
        
        #print response.status_code
        #print response.json()        

    def get_table(self, table_id):
        response = requests.get("%s/tables/%s/" % (QUILT_URL, table_id),
                                headers=HEADERS,
                                auth=self.auth)        
        
        print response.status_code
        return Table(self.auth, response.json())

        
        
    
