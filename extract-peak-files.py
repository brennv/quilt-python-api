#!/path/to/python

import re
import sys


def main(argv):
  if len(argv) != 2:
    sys.stderr.write("Usage: %s <filename>\n" % argv[0])
    return 1

  fname = argv[1]
  f = open(fname, 'r')
  raw = f.read()
  
  peak_files = re.compile('\"(.*peak\.gz)\"', re.IGNORECASE)
  tokens = raw.split()
  for t in tokens:
    m = peak_files.match(t)
    if m:
      sys.stdout.write("%s\n" % m.group(1))

  return 1


if __name__ == '__main__':
  sys.exit(main(sys.argv))
