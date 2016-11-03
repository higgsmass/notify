import logging, os, sys

# ----------------------------------------------------------------------------------------------------
## initialize logger (default to stdout). if successful return handle to logger
## logs will be rotated after file exceeds maxbytes
## input: path to log output file (string), max size (in MB) allowed before rotating log output file (int)
## output: file handle to logger
#---------------------------------------------------

def logInit(level, outputLocation="stdout", modulename="root", maxmb=5):
    from logging import handlers
    root = logging.getLogger(modulename)
    if level == 'VERBOSE' or level == None:
        level = 'INFO'
    root.setLevel(getattr(logging, level))

    ch = None
    if outputLocation == "stdout":
        ch = logging.StreamHandler(sys.stdout)
    else:
        pglogdir = os.path.dirname(os.path.abspath(outputLocation))
        if not os.path.isdir(pglogdir):
            print 'cannot find '+pglogdir+' quitting'
            return None
        try:
            fp = open(os.path.abspath(outputLocation), 'a')
        except IOError as e:
            raise
            print 'Cannot write to '+outputLocation+' using stdout as default'
            return None
        ch = logging.handlers.RotatingFileHandler(os.path.abspath(outputLocation), maxBytes=(1048576*maxmb), backupCount=7)
    if not ch:
        return ch
    ch.setLevel(getattr(logging, level))
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)
    return root

