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
import traceback
import distutils
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
        self.ets_codes_found = None
        self.sym_link = ''
        self.class_path = '.:'
        self.upd_cbxs = None
        self.msg_type = { True:'SUCCESS', False:'FAILURE' }
        try:
            self.class_path += os.environ['CLASSPATH'] + ':'
        except KeyError:
            pass
        self.user = os.environ['USER']
        self.valid_sites = []
        self.std_out = { 'create_batch': None, 'ets_valid':None, 'request_set':None }
        self.std_err = { 'create_batch': None, 'ets_valid':None, 'request_set':None }

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

        opt_valid = True
        self.opt_path = 'one'

        msg_head = 'Command line Options:\n'

        for arg, value in sorted(vars(self.options).items()):
            msg_head += '%s = %r\n' % (arg, value)

        self.log.info(msg_head)

        msg_head = 'Validating command line options:\n'


        if self.options.ets_validate or self.options.run_request:
            self.opt_path = 'two'
            if not self.options.batch_id:
                msg  = '\nERROR: Cannot proceed without batch ID with the following options.\n\t--ets-validate, --run-requestset\nSpecify [option] --batch-id <ID>'
                opt_valid = False
            if self.options.blsite or self.options.blsize:
                msg = '\nERROR: Cannot start a new batch in this mode. \n\tEITHER Remove --batch-size and --site options and retry.'
                msg += '\tOR Remove --ets-validate, --run-requestset and retry'
                msg += '\n\tNOTE If running a new batch, --ets-validate, --run-requestset are TRUE by default. You can set it to yes/no in the configuration file'
                opt_valid = False
        else:
            if not (self.options.blsite and self.options.blsize):
                msg = '\nERROR: Require --batch-size and --site to proceed with new batch'
                opt_valid = False
            else:
                if self.options.batch_id:
                    msg = '\nERROR: --batch-size and --site options will start a new batch. Remove --batch-id <ID> option'
                    opt_valid = False

        msg_head +=  self.msg_type [ opt_valid ]
        if opt_valid:
            msg_head += (', OPT_PATH: %s' % self.opt_path)
            if self.options.batch_id:
                msg_head += (', BATCH_ID: %d\n' % self.options.batch_id)

        if not opt_valid:
            sys.stderr.write(msg+'\n')
            self.log.error(msg)
            sys.exit(1)
        else:
            self.log.info(msg_head)

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


    def run_create_batch(self):
        cmd = [ 'java', '-Xms512m', '-Xmx1024m', self.load_opts['javaclass'] ]
        cmd.append(self.options.blsite)
        cmd.append(str(self.options.blsize))

        self.log.debug(' '.join(cmd))
        (self.std_out['create_batch'], self.std_err['create_batch'], p) =  os_system_command( ' '.join(cmd), self.run_env)
        if not p.returncode == 0:
            self.log.error(self.std_err['create_batch'])
            sys.exit(p.returncode)
        else:
            self.log.info('\n'+('-'*20)+'\n'+self.std_out['create_batch'])

        self.status_createbatch()


    def run_ets_validate(self, batch_id):

        self.log.info('Running ETS Validation')
        sys.stdout.write('Running ETS Validation\n')

        if batch_id == None:
            if 'batch' in self.batch_create_data.keys():
                batch_id = self.batch_create_data['batch'][1]['batch']
            else:
                sys.log.error('batch id not found, cannot proceed with ETS validation')
                sys.exit(1)

        sql_path = distutils.sysconfig.get_python_lib()
        cmd = [ 'sqlplus', '-S', 'apps/apps', '@' + os.path.join(sql_path, 'notify/plsql/ets_validate.sql'), str(batch_id) ]

        self.log.debug(' '.join(cmd))

        (self.std_out['ets_valid'], self.std_err['ets_valid'], p) =  os_system_command( ' '.join(cmd), self.run_env)
        if not p.returncode == 0:
            self.log.error(self.std_err['ets_valid'])
            sys.exit(p.returncode)
        else:
            self.log.info('\n'+('-'*20)+'\n'+self.std_out['ets_valid'])

        self.ets_codes_found = False
        key = self.load_opts['ets_key']
        codes = dict()

        for line in self.std_out['ets_valid'].split('\n'):
            if key in line:
                lsp = [ i.strip() for i in line.split('=') ]
                if len(lsp) > 1:
                    codes[ key ] = lsp[1]
                break

        if not key in codes:
            log.error('%s: no such key/pattern found for validation of ETS codes' % key)

        self.batch_create_data.update( { key : [ lsp[1], codes ] }    )

        try:
            self.ets_codes_found = int(self.batch_create_data[key][1][key]) > 0
        except ValueError:
            log.error('Unexpected error in converting ETS error code count')
            self.ets_codes_found = True



    def run_request_set(self, batch_id):

        self.log.info('Running request set')
        sys.stdout.write('Running request set\n')

        if batch_id == None:
            if 'batch' in self.batch_create_data.keys():
                batch_id = self.batch_create_data['batch'][1]['batch']
            else:
                sys.log.error('batch id not found, cannot proceed with running request set')
                sys.exit(1)


        if self.ets_codes_found == None:
            self.run_ets_validate(batch_id)

        if self.ets_codes_found == True:
            k1 = self.load_opts['ets_key']
            self.log.warn('%s: ETS codes need to be added for batch ID %s' % (self.batch_create_data[k1][1][k1], str(batch_id)))
            sys.stderr.write('WARN:  %s: ETS codes need to be added for batch ID %s' % (self.batch_create_data[k1][1][k1], str(batch_id)))
            sys.exit(1)

        sql_path = distutils.sysconfig.get_python_lib()
        cmd = [ 'sqlplus', '-S', 'apps/apps', '@'+ os.path.join(sql_path, 'notify/plsql/run_request_set.sql'), str(batch_id) ]

        self.log.debug(' '.join(cmd))
        (self.std_out['request_set'], self.std_err['request_set'] , p) =  os_system_command( ' '.join(cmd), self.run_env)
        if not p.returncode == 0:
            self.log.error(self.std_err['request_set'])
            sys.exit(p.returncode)
        else:
            self.log.info('\n'+('-'*20)+'\n'+self.std_out['request_set'])


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
                        self.log.debug(self.valid_sites)
        except IOError as err:
            self.log.error( 'I/O error {0} {1}'.format(err.errno, err.strerror))
            sys.exit(err.errno)

        if  self.opt_path == 'one':
            ## check if site is one among allowed (valid) sites
            if self.options.blsite.strip() not in self.valid_sites:
                self.log.error( 'not a valid site: %s. sites must be one of [ %s ]' % (self.options.blsite, ', '.join(self.valid_sites)) )
                sys.exit(1)

            ## ensure batch size > 0
            if self.options.blsize < 1:
                self.log.error('invalid batch size: %d, must be a positive whole number' % self.options.blsize)
                sys.exit(1)

            self.log.info('SITE = %s, BATCH_SIZE = %d\n' % (self.options.blsite.strip(), self.options.blsize))

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


    def status_createbatch(self):
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

        num_found = 0


        for line in self.std_out['create_batch'].split('\n'):
            for pat in patterns:
                multiline = True in patterns[pat]
                res = None
                mat_grp = patterns[pat][0].match(line)
                if mat_grp:
                    res = mat_grp.groupdict()
                if res:
                    if pat in self.batch_create_data and multiline:
                        ## assume integer addition and try to add the two
                        #self.log.warn('already exists: %s' % line)
                        try:
                            res[pat] = str( int(self.batch_create_data[pat][1][pat]) + int(res[pat]) )
                        except ValueError:
                            pass
                    else:
                        self.batch_create_data.update( { pat:[line, res]})
                        num_found += 1

        self.batch_create_data.update( { 'bluser':[self.user, {'bluser':self.user} ]})
        num_found += 1

        required = ['fails', 'succs', 'end_date', 'start_date', 'bluser', 'procs', 'batch']

        for it in required:
            if not it in self.batch_create_data.keys():
                msg = 'ERROR: Required data not found with key: %s' % it
                self.log.error(msg)
                self.stderr.write(msg+'\n')
                sys.exit(1)


    def update_createbatch(self, append = True):

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

        self.upd_cbxs = result.get('updates').get('updatedRange')

        msg_stat = self.msg_type [ self.batch_create_data['fails'][1]['fails'].strip() == '0' ]

        msg_body = '\nbatch-id: %s %s\npass: %s,fail: %s, proc: %s\nstart: %s\nend: %s\n' % ( self.batch_create_data['batch'][1]['batch'] , msg_stat,
                self.batch_create_data['succs'][1]['succs'].strip(),
                self.batch_create_data['fails'][1]['fails'].strip(),
                self.batch_create_data['procs'][1]['procs'].strip(),
                self.batch_create_data['start_date'][1]['start_date'].strip(),
                self.batch_create_data['end_date'][1]['end_date'].strip(),
                )

        self.notifier.message('bc_notify_counts', msg_body)

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

            ## path one is creating a new batch(man), ets-validate(opt) and run-request set(opt)
            if self.opt_path == 'one':
                self.run_create_batch()
                self.update_createbatch()
                if self.load_opts['bc_ets_validate'].upper()[0] == 'Y':
                    self.run_ets_validate(None)
                if self.load_opts['bc_run_requestset'].upper()[0] == 'Y':
                    self.run_request_set(None)

            elif self.opt_path == 'two':
                if self.options.ets_validate:
                    self.run_ets_validate(self.options.batch_id)
                if self.options.run_request:
                    self.run_request_set(self.options.batch_id)
        except:
            pass
        finally:
            self.cleanup()
            lock.release()
            self.log.info('Processing complete')

