#!/usr/bin/python
#python cli utility for creating data sets (from files) on quiltdata.com
import argparse
import getpass
import json
import os
import requests
import sys

from requests.auth import HTTPBasicAuth

HEADERS = {'Content-Type': 'application/json', 'Accept': 'application/json'}


def create(argv):
  parser = argparse.ArgumentParser(description='Create a data set on Quilt.')
  parser.add_argument('-e', '--endpoint', default='https://quiltdata.com', help='API endpoint root URL (without terminating "/")')
  parser.add_argument('-u', '--user', default=None, help='Quilt username', required=True)
  parser.add_argument('-n', '--name', default=None, help='Data set name', required=True)
  parser.add_argument('-d', '--description', default=None, help='Data set description')
  parser.add_argument('-f', '--file', default=None, help='Path to CSV, TXT, XLS, BED, or other supported format')
  parser.add_argument('-p', '--public', default=False, help='True for public, False for private. Private is default.')
  parser.add_argument('-x', '--password', default=None, help='Password. For script-to-script use only. Not for commandline use, as password would remain in shell history.')

  args = parser.parse_args(argv)
  passwd = args.password or getpass.getpass()

  signature = None
  if (args.file):
    file_name = os.path.basename(args.file)
    #get signed S3 URL
    response = get_upload_url(file_name, args, passwd)
    print response.json()
    check_response(response, 'file upload failed')
    signature = json.loads(response.json())
    #request header
    headers = {
      'Content-Type': 'text/plain',
      'x-amz-acl': signature['x-amz-acl']
    }
    destination = signature['signed_request']
    #local file handle
    data = open(args.file, 'rb')
    #upload file
    upload = requests.put(destination, data=data, headers=headers)
    check_response(upload, 's3 signing failed')
    #end if(args.file)

  #assemble schema (.columns is required, even if empty)
  #TODO specify format for columns 
  #See https://github.com/quiltdata/python-api/issues/41
  file_path = signature['path'] if signature and signature['path'] else None
  schema = {
    'csvfile': file_path,
    'name': args.name,
    'description': args.description,
    'columns': [],
    'is_public': args.public
  }
  #create data set
  endpoint = "%s/tables/" % (args.endpoint)
  create = requests.post(endpoint,
    auth=HTTPBasicAuth(args.user, passwd),
    data=json.dumps(schema),
    headers=HEADERS)
  check_response(create, 'problem creating data set')
#end main

# get_upload_url fetch signed URL from backend
def get_upload_url(file_name, args, passwd, file_type='text/plain'):
  print(file_name)
  data = {'fileName': file_name, 'fileType': file_type}
  endpoint = "%s/files/" % (args.endpoint)
  #use post not get to avoid enumerating all files in response
  return requests.post(
            endpoint,
            auth=HTTPBasicAuth(args.user, passwd),
            params=data,
            files={'file': file_name},
            headers=HEADERS)


def check_response(response, msg): 
  if not response.ok:
    sys.stderr.write('Oops, %s\n' % msg)
    detail = json.loads(response.text)['detail']
    sys.stderr.write('%s %s: %s\n' % (response.status_code, response.reason, detail))
    sys.exit()


if __name__ == "__main__":
  create(sys.argv[1:])
