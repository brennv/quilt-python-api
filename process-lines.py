#!/path/to/python

import os
import re
import sys


ENDPOINT = 'https://quilt-heroku.herokuapp.com/'
CORE_TAGS = ['Human', 'ENCODE', 'ChIP-seq', 'hg19']
CORE_DESC = 'Sources\nhttps://www.encodeproject.org/\nhttp://hgdownload.cse.ucsc.edu/goldenPath/hg19/encodeDCC/wgEncodeBroadHistone/'
PUBLIC = True

def main(argv):
  if len(argv) != 2:
    sys.stderr.write("Usage: %s <filename>\n" % argv[0])
    return 1

  fname = argv[1]
  lines = [line.strip() for line in open(fname)]
  
  for l in lines:
    name = os.path.splitext(l)[0]
    pretags = re.findall('[A-Z][^A-Z]*', name)
    #pull out tags already covered by core tags
    pretags.remove('Encode')
    pretags.remove('Broad')
    tags = map(lambda x: '#' + x, CORE_TAGS + pretags)
    description = ' '.join(tags) + '\n' + CORE_DESC
    print("%s\n%s" % (l, description))

  return 1


if __name__ == '__main__':
  sys.exit(main(sys.argv))
