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

