#!/usr/bin/python
import re
from subprocess import call
import sys


def main(argv):
  if len(argv) != 4:
    sys.stderr.write("Usage: %s <list of files.txt> <root url> <output directory>\n" % argv[0])
    return 1

  fname = argv[1]
  url = argv[2]
  my_dir = './' + argv[3] + '/'
  f = open(fname, 'r')
  raw = f.read()
  
  tokens = raw.split()
  for t in tokens:
    call(['curl', '-o', my_dir + t, url + t])

  return 1


if __name__ == '__main__':
  sys.exit(main(sys.argv))
