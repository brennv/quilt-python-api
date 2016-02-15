import argparse
import boto3
import getpass
import json
import mimetypes
import os
import requests
import sys

from requests.auth import HTTPBasicAuth

HEADERS = {
  "Content-Type": "application/json",
  "Accept": "application/json"
}


def main(argv):
  parser = argparse.ArgumentParser(description='Create a Quilt data set.')
  parser.add_argument('-e', '--endpoint', default='https://quiltdata.com', help='API endpoint root URL (without terminating "/")')
  parser.add_argument('-u', '--user', default=os.environ['USER'], help='Quilt username')
  parser.add_argument('-n', '--name', default='test_data_set', help='Data set name')
  parser.add_argument('-d', '--description', default='', help='Data set description')
  parser.add_argument('-f', '--file', default='', help='Path to CSV, TXT, TSV, BED, or other supported format')
  parser.add_argument('-p', '--public', default=False, help='Publicly visible? (True or False). Default=False.')
  args = parser.parse_args(argv)
  passwd = getpass.getpass()


  def get_upload_url(file_name, file_type='text/plain'):
    data =  {
      'fileName': file_name,
      'fileType': file_type
    }
    print data
    endpoint = "%s/s3args/" % (args.endpoint)

    return requests.get(endpoint,
              auth=HTTPBasicAuth(args.user, passwd),
              params=data,
              headers=HEADERS)
  #end get_upload_url


  #get signed S3 URL
  file_name = os.path.basename(args.file)
  file_type = mimetypes.guess_type(file_name)[0]
  response = get_upload_url(file_name, file_type)
  if response.status_code != 200:
    print 's3 signing error: %s' % (response.text)
  signature = json.loads(response.json())
  #request header
  headers = HEADERS.copy()
  headers['Content-Type'] = 'text/plain'
  headers['x-amz-acl'] = signature['x-amz-acl']
  destination = signature['signed_request']
  #schema (columns is required)
  schema = {
    'csvfile': signature['path'],
    'name': args.name,
    'description': args.description,
    'columns': [],
    'is_public': args.public
  }  
  #local file
  files = {file_name: open(args.file, 'r')}
  #upload/put file with request headers
  print destination
  print files
  print headers
  upload = requests.put(destination, files=files, headers=headers)
  if upload.status_code != 200:
    print 's3 signing error: %s' % (response.text)
#end main


if __name__ == "__main__":
  main(sys.argv[1:])
