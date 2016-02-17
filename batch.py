#!/usr/bin/python
import getpass
import os
import re
import shlex
import sys

import dataset

ENDPOINT = 'https://quilt-heroku.herokuapp.com/'
CORE_TAGS = ['Human', 'ENCODE', 'ChIP_seq', 'hg19']
CORE_DESC = 'Sources\nhttps://www.encodeproject.org/\nhttp://hgdownload.cse.ucsc.edu/goldenPath/hg19/encodeDCC/wgEncodeBroadHistone/'
PUBLIC = True

def process(argv):
  if len(argv) != 2:
    sys.stderr.write("Usage: %s <filename>\n" % argv[0])
    return 1

  #password
  passwd = getpass.getpass()
  fname = argv[1]
  #process line by line
  lines = [line.strip() for line in open(fname)]
  for l in lines:
    name = os.path.splitext(l)[0]
    pretags = re.findall('[A-Z][^A-Z]*', name)
    #pull out tags already covered by core tags
    pretags.remove('Encode')
    pretags.remove('Broad')
    tags = map(lambda x: '#' + x, CORE_TAGS + pretags)
    description = ' '.join(tags) + '\n' + CORE_DESC
    args = "-u akarve -n %s -d '%s' -f downloads/%s -p True -x '%s'" % (name, description, l, passwd)
    argv = shlex.split(args)
    #create data set on Quilt 
    dataset.create(argv)

  return 1


if __name__ == '__main__':
  sys.exit(process(sys.argv))
