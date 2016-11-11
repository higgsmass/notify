import os
import re
import sys
import csv
import time
import glob
import stat
import fcntl

import fnmatch
import logging
import datetime
import tempfile
import subprocess
import collections

## local imports
import config
import logger
import goose
import notify

def os_system_command(cmd, m_env=None):
    res = None
    out = ''
    err = None

    try:
        if m_env == None:
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env = m_env)

        for line in iter(p.stdout.readline, b''):
            out += line
            sys.stdout.write(line)
        err = p.communicate()[1]
        p.wait()
    except OSError:
        sys.stderr.write('ERROR: encountered in command\n\'%s\'' % cmd)
        sys.stderr.write(p.communicate()[1])
        sys.exit(p.returncode)

    return (out, err, p)


## acquire/release exclusive lock while processing bulk-loads for this datatype
class Lock:

    def __init__(self, filename, pars):
        self.filename = filename
        # This will create it if it does not exist already
        self.handle = open(filename, 'w')
        self.handle.write('\n'.join(pars))

    # Bitwise OR fcntl.LOCK_NB if you need a non-blocking lock
    def acquire(self):
        fcntl.flock(self.handle, fcntl.LOCK_EX)

    def release(self):
        fcntl.flock(self.handle, fcntl.LOCK_UN)

    def __del__(self):
        self.handle.close()

class loader(object):

    def __init__(self, parser):

        self.parser = parser
        self.heading = "bulk_load"
        self.options = parser.parse_args(sys.argv[1:])
        self.config_path = None
        self.config_vars = None
        self.pid = None
        self.lockfile = None
        self.sym_link = ''
        self.class_path = '.:'
        try:
            self.class_path += os.environ['CLASSPATH'] + ':'
        except KeyError:
            pass
        self.user = os.environ['USER']
        self.valid_sites = []
        self.std_out = None
        self.std_err = None

        self.batch_create_data = dict()

        sys.stderr.write("\n--> Reading configuration file: %s\n" % self.options.conf)
        try:
            with open (self.options.conf, 'r') as f:
                f.close()
            self.config_path = self.options.conf
            self.config_vars = config.getConfigParser(self.config_path)
        except IOError, err:
            sys.stderr.write('ERROR: %s\n' % str(err))
            traceback.print_exc()
            sys.exit(err.errno)

        self.load_opts = config.getConfigSectionMap( self.config_vars, self.heading )
        self.log = logger.logInit(self.options.logLevel, self.load_opts['log_path'], type(self).__name__)

        if self.options.show:
            self.printconfig(True)

        self.pid = os.getpid()
        self.lockfile = os.path.join( self.load_opts['temp_path'], self.load_opts['category'] + '.lock')

        self.gapi = goose.sheets(parser, self.config_vars, self.log)
        self.service = self.gapi.service_start()

        self.notifier = notify.notify(parser, self.config_vars, self.log)

    def printconfig(self, defaults):
        if defaults:
            sys.stderr.write('\n----------------------[ DEFAULT ]----------------------\n')
            for k,v in self.config_vars.defaults().iteritems():
                sys.stderr.write( '%-25s: %s\n' % (k, v))

        qq = config.getConfigSectionMap(self.config_vars, self.heading)
        sys.stderr.write('\n----------------------[ %s ]----------------------\n' % self.heading)
        for j in qq:
            if j not in self.config_vars.defaults().keys():
                sys.stderr.write( '%-25s: %s\n' % (j, qq[j]))


    def capture_env(self, env_vars):
        ## base env
        self.run_env = dict(os.environ)

        ## oracle env
        clpath_found = False
        for line in env_vars.split('\n'):
            if not '=' in line:
                continue
            try:
                key, val = line.split('=', 1)

                ## append classpath if key has it
                if key == 'CLASSPATH':
                    val += ':' + self.class_path
                    clpath_found = True
                self.run_env.update({ key:val })
            except ValueError, err:
                self.log.warn('caught an exception in env \'%s\'\n%s' % (line, err))
                pass

        ## append classpath if not already done
        if not clpath_found:
            self.run_env.update({ 'CLASSPATH':self.class_path })

        if self.options.logLevel == 'VERBOSE':
            out = ''
            for key in self.run_env:
                out += key+'='+self.run_env[key]+'\n'
            self.log.info(out)


    def execute(self):
        cmd = [ 'java', '-Xms512m', '-Xmx1024m', self.load_opts['javaclass'] ]
        cmd.append(self.options.blsite)
        cmd.append(str(self.options.blsize))

        self.log.info(' '.join(cmd))
        (self.std_out, err, p) =  os_system_command( ' '.join(cmd), self.run_env)
        if not p.returncode == 0:
            self.log.error(err)
            sys.exit(p.returncode)
        self.create_batch_status()


    def validate(self):

        cl_suffix = '.class'
        delim = '/'

        ## validate paths
        for item in [ self.load_opts['basepath'], self.load_opts['libpath'], self.load_opts['confpath'] ]:
            if not os.path.exists(item):
                self.log.error( 'no file/directory matching: %s ' % item )
                sys.exit(1)

        (jpath, jfile) = os.path.split( self.load_opts['javaclass'].replace('.', delim) )
        jclass_found = False

        ## set symlink for executing java program
        try:
            clp = fnmatch.filter (os.listdir(os.path.join(self.load_opts['basepath'] , jpath)), jfile + cl_suffix )[0]
            jclass_found = clp == (jfile + cl_suffix)
        except IndexError:
            pass

        if not jclass_found:
            self.log.error( 'no file matching: %s' %  jfile + cl_suffix)
            sys.exit(1)

        linkname = jpath.split(delim)[0]
        self.linkdir = os.path.join( os.getcwd(), linkname)
        self.sym_link = os.path.join(self.load_opts['basepath'] , linkname)
        link_exists = False
        try:
            link_exists = stat.S_ISLNK(os.lstat(self.linkdir).st_mode)
        except OSError:
            pass
        if link_exists:
            self.log.warn('symbolic link %s exists. trying to remove it' % self.linkdir)
            self.cleanup()
        try:
            if self.options.logLevel == 'VERBOSE':
                self.log.info('creating symbolic link %s -> %s ' % (self.linkdir, self.sym_link))
            os.symlink( self.sym_link, self.linkdir )
        except OSError:
            self.log.error('cannot symlink: %s -> %s' % (self.linkdir, self.sym_link))

        ## setup CLASSPATH environment variable (lib/*.jar and config)
        for item in glob.glob( os.path.join(self.load_opts['libpath'], '*.jar')):
            self.class_path += item + ':'

        self.class_path += self.load_opts['confpath']
        if self.options.logLevel == 'VERBOSE':
            self.log.info('%s' % self.class_path)

        ## read list of valid sites from properties file
        try:
            with open( self.load_opts['validation'], 'rb') as f:
                for line in f:
                    if self.load_opts['site_list'] in line:
                        self.valid_sites = line.split('=')[1].strip().split('|')
                        self.log.info(self.valid_sites)
        except IOError as err:
            self.log.error( 'I/O error {0} {1}'.format(err.errno, err.strerror))
            sys.exit(err.errno)

        ## check if site is one among allowed (valid) sites
        if self.options.blsite.strip() not in self.valid_sites:
            self.log.error( 'not a valid site: %s. sites must be one of [ %s ]' % (self.options.blsite, ', '.join(self.valid_sites)) )
            sys.exit(1)

        ## ensure batch size > 0
        if self.options.blsize < 1:
            self.log.error('invalid batch size: %d, must be a positive whole number' % self.options.blsize)
            sys.exit(1)

        ## append additional environment (capture default env set by ORACLE apps)
        self.log.debug('setting up environment variables for ORACLE apps: %s' % self.load_opts['environment'])
        try:
            p = subprocess.Popen( ['bash', '-c', "trap 'env' exit; source \"$1\" > /dev/null 2>&1",
                "_", self.load_opts['environment'] ], shell=False, stdout=subprocess.PIPE)
            (env_vars, cerr) = p.communicate()
            if not p.returncode == 0:
                self.log.error(cerr)
                sys.exit(p.returncode)
        except OSError:
            self.log.error(cerr)
            sys.exit(p.returncode)

        self.capture_env(env_vars)


    def create_batch_status(self):
        pat_base = [ r'(?P<time>.+)', r'\[(?P<process>\S+)\]', r'(?P<status>Line: [0-9]+\s+INFO\s+\-)' ]
        ## regex patterns to extract from output

        ## Create Batch Date/Time Started (tab = SITE-DataType, column = D)
        pat_start = pat_base + [ r'(?P<fmsg>Processing Errors.*)' ]

        ## Batch ID (tab = SITE-DataType, column = C)
        pat_batch = pat_base + [ r'(?P<fmsg>Created New Batch ID\s+\-)', r'(?P<batch>.*[0-9]+.*)' ]

        ## Count Failed (tab = SITE-DataType, column = O)
        pat_fail = [ r'(?P<fmsg>.*Total Fail.*=)', r'(?P<fails>.*[0-9]+.*)' ]

        ## Count Successful (column = N)
        pat_succ = [ r'(?P<fmsg>.*Total Succ.*=)', r'(?P<succs>.*[0-9]+.*)' ]

        ## Total Processed (column = P)
        pat_proc = [ r'(?P<fmsg>.*Total Proc.*=)', r'(?P<procs>.*[0-9]+.*)' ]

        ## Create Batch Date/Time Completed (tab = SITE-DataType, column = E)
        pat_finish = pat_base + [ r'(?P<fmsg>Closed Source Connections.*)']

        ## False = No multi-line, True = multi-line patterns, attempt to add results
        patterns = { 'start_date': [ re.compile(r'\s+'.join(pat_start).replace('time','start_date',1)), False],
                'fails':[ re.compile(r'\s+'.join(pat_fail)), True],
                'succs':[ re.compile(r'\s+'.join(pat_succ)), False],
                'procs':[ re.compile(r'\s+'.join(pat_proc)), False ],
                'batch':[ re.compile(r'\s+'.join(pat_batch)), False ],
                'end_date':[ re.compile(r'\s+'.join(pat_finish).replace('time', 'end_date',1)), False ]
                }


        for line in self.std_out.split('\n'):
            for pat in patterns:
                multiline = True in patterns[pat]
                res = None
                mat_grp = patterns[pat][0].match(line)
                if mat_grp:
                    res = mat_grp.groupdict()
                if res:
                    if pat in self.batch_create_data and multiline:
                        ## assume integer addition and try to add the two
                        self.log.warn('already exists: %s' % line)
                        try:
                            res[pat] = str( int(self.batch_create_data[pat][1][pat]) + int(res[pat]) )
                        except ValueError:
                            pass
                    else:
                        self.batch_create_data.update( { pat:[line, res]})

        self.batch_create_data.update( { 'bluser':[self.user, {'bluser':self.user} ]})

    def batch_create_status(self, append = True):

        self.bcs_opts = config.getConfigSectionMap( self.config_vars, 'create-batch' )

        upcols = self.bcs_opts['column_updates'].split('|')
        mpcols = self.bcs_opts['column_map'].split('|')

        result = self.service.spreadsheets().values().get(
                    spreadsheetId = self.gapi.get_id(), range = self.bcs_opts['column_range']).execute()
        row = result.get('values')

        keys = row[0]


        status_row = collections.OrderedDict()
        for i in keys:
            status_row[i] = ''

        for i in range(0, len(upcols)):
            key = mpcols[i]
            head = upcols[i]
            status_row[head] = self.batch_create_data[key][1][key]

        data = [ v for k,v in status_row.iteritems() ]

        body_append = {
                'values': [ data ]
                }

        result = self.service.spreadsheets().values().append(
                 spreadsheetId = self.gapi.get_id(), valueInputOption='USER_ENTERED',
                 range = self.bcs_opts['column_range'], insertDataOption='INSERT_ROWS',
                 body=body_append).execute()


        msg_body = 'batch %s completed at %s [P|F|T] = [%s|%s|%s]' % ( self.batch_create_data['batch'][1]['batch'] ,
                self.batch_create_data['end_date'][1]['end_date'], self.batch_create_data['succs'][1]['succs'],
                self.batch_create_data['fails'][1]['fails'], self.batch_create_data['procs'][1]['procs']
                )

        self.notifier.message('batch_create_notify', msg_body)

    def cleanup(self):
        try:
            os.unlink( self.linkdir )
        except OSError:
            self.log.error('cannot remove file/link: %s' % self.linkdir)
            sys.exit(1)


    def run(self):
        pars = [ 'user = %s' % self.user, 'datatype = %s' % self.load_opts['category'], 'pid = %s' % self.pid, datetime.datetime.now().strftime("last_run = %F %H:%M:%S\n") ]
        lock = Lock(self.lockfile, pars)
        try:
            lock.acquire()
            self.validate()
            self.execute()
            self.batch_create_status()
        except:
            pass
        finally:
            self.cleanup()
            lock.release()
            self.log.info('Processing complete')

