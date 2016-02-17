#!/usr/bin/python
import argparse
import getpass
import json
import os
import requests
import sys

from requests.auth import HTTPBasicAuth

HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}


def create(argv):
  parser = argparse.ArgumentParser(description='Create a Quilt data set.')
  parser.add_argument('-e', '--endpoint', default='https://quiltdata.com', help='API endpoint root URL (without terminating "/")')
  parser.add_argument('-u', '--user', default=os.environ['USER'], help='Quilt username')
  parser.add_argument('-n', '--name', default='test_data_set', help='Data set name')
  parser.add_argument('-d', '--description', default='', help='Data set description')
  parser.add_argument('-f', '--file', default='', help='Path to CSV, TXT, TSV, BED, or other supported format')
  parser.add_argument('-p', '--public', default=False, help='True for public, False for private. Private is default.')
  parser.add_argument('-x', '--password', default=None, help='Password. NOT for commandline use, as password will be in shell history.')
  args = parser.parse_args(argv)

  passwd = None
  if(args.password == None):
    passwd = getpass.getpass()
  else:
    passwd = args.password

  #get_upload_url fetch signed URL from backend
  def get_upload_url(file_name, file_type='text/plain'):
    data = {'fileName': file_name, 'fileType': file_type}
    endpoint = "%s/s3args/" % (args.endpoint)
    return requests.get(endpoint,
              auth=HTTPBasicAuth(args.user, passwd),
              params=data,
              headers=HEADERS)
  #end get_upload_url


  file_name = os.path.basename(args.file)
  #get signed S3 URL
  response = get_upload_url(file_name)
  check_response(response, 'problem signing file')
  signature = json.loads(response.json())
  #request header
  headers = {
    'Content-Type': 'text/plain',
    'x-amz-acl': signature['x-amz-acl']
  }
  destination = signature['signed_request']
  #local file handle
  files = {file_name: open(args.file, 'rb')}
  data = open(args.file, 'rb')
  #upload file
  upload = requests.put(destination, data=data, headers=headers)
  check_response(upload, 's3 signing error')
  #assemble schema (columns is required)
  schema = {
    'csvfile': signature['path'],
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


def check_response(response, msg): 
  if response.status_code != 200:
    sys.stderr.write('%s: %s\n\t%s\n' % (msg, response, response.text))


if __name__ == "__main__":
  create(sys.argv[1:])
