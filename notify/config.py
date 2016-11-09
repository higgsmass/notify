## -------------------------------------------
## module   : parse config a la python
## package  : cdwlib
## author   : kaushik@mailbox.sc.edu
## created  : Tue Mar  8 08:17:12 EST 2016
## vim      : ts=4

import ConfigParser
import StringIO
import csv, os, re
import sys
# ----------------------------------------------------------------------------------------------------
## instantiate postgres style dialect
class pgDialect(csv.Dialect):
    quotechar='\x07'
    delimiter = ','
    lineterminator = '\n'
    doublequote = False
    skipinitialspace = True
    quoting = csv.QUOTE_NONE
    escapechar = '\\'


# ----------------------------------------------------------------------------------------------------
# Get and process config (.ini) file and convert to configuration object
def getConfigParser(iniPath):
  config = None
  if not os.path.isfile(iniPath):
    return config
  try:
    config = ConfigParser.SafeConfigParser()
    config.read(iniPath)
  except:
    pass
  return config


# ----------------------------------------------------------------------------------------------------

# Read configuration (ConfigParser) object and convert to python dictionary
def getConfigSectionMap(config, section):
  cfgmap = {}

  if not config:
    return cfgmap

  options = config.options(section)
  for opt in options:
    try:
      cfgmap[opt] = config.get(section, opt)
      if cfgmap[opt] == -1:
        pass
    except:
      cfgmap[opt] = None

  return cfgmap


#-------------------------------------------------------------------------
# Parse csv file, ignore newlines '\n' or comments '#' at the beginning
# of each line. Return a list of dictionary objects
def parseCSV(path):


    rows = []

    ## check if path/csv file are ok
    if not os.path.isfile(path):
        return None

    try:
        ## clean up newlines and comment lines
        data = ''; lines = None
        with open(path, 'rb') as csvfile:
            lines = filter(lambda x: not re.match(r'^\s*$', x), csvfile)
        if lines:
            line = filter(lambda lines: lines[0]!='#', lines)
        data += ''.join(line)

        ## sniff dialect and parse lines
        if not data == '':
            reader = csv.DictReader(StringIO.StringIO(data), dialect=pgDialect)
            rows = [ row for row in reader ]
    except IOError:
        raise
        return None

    ## return rows
    return rows




