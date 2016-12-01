import os
import re
import sys
import pwd
import csv
import time
import glob
import stat
import fcntl

import fnmatch
import logging
import sqlite3
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

from helper import *

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
        self.bcs_opts = None
        self.linkdir = None
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

        self.sql_path = os.path.join ( distutils.sysconfig.get_python_lib(), 'notify/plsql' )

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

        dbname = os.path.join(self.load_opts['db_path'], ('.' + self.load_opts['category'] + '.db'))
        self.db = SQLiteDB(dbname)
        if not self.db:
            self.log.error('ERROR initializing database for messaging/authentication');
            sys.exit(1)

        ## load db with authy/twilio details
        try:
            with open(os.path.join(self.sql_path, 'create_authuser.sql')) as f:
                lines = ''.join(f.readlines())
                self.db.handle().executescript(lines)
        except sqlite3.Error as e:
            msg = 'ERROR: ' + ' '.join(e.args)+'\n'
            sys.stderr.write(msg)
            self.log.error(msg)
        except IOError as err:
            self.log.error( 'I/O error {0} {1}'.format(err.errno, err.strerror))
            sys.exit(err.errno)

        ## validate user
        current_user = self.user.upper().strip()
        config_user = self.load_opts['user'].upper().strip()
        if current_user == config_user:
            msg = '\n--> Running as user: %s\n' % self.user
            sys.stdout.write(msg)
        else:
            msg = 'User profile mismatch. Are you sure you are \'%s\'? Modify configuration and set \'user\' parameter to %s\n' % (config_user.lower(), current_user.lower())
            self.log.error(msg)
            sys.stderr.write(msg)
            sys.exit(1)

        self.pid = os.getpid()
        self.lockfile = os.path.join( self.load_opts['temp_path'], self.load_opts['category'] + '.lock')

        self.gapi = goose.sheets(parser, self.config_vars, self.log)
        self.service = self.gapi.service_start()

        self.notifier = notify.notify(parser, self.db, self.user, self.config_vars, self.log)


    def printconfig(self, defaults):
        if defaults:
            sys.stderr.write('\n----------------------[ DEFAULT ]----------------------\n')
            for k,v in self.config_vars.defaults().iteritems():
                sys.stderr.write( '%-25s: %s\n' % (k, v))

        qq = config.getConfigSectionMap(self.config_vars, self.heading)
        sys.stderr.write('\n----------------------[ %s ]----------------------\n' % self.heading)
        for j in qq:
                sys.stderr.write( '%-25s: %s\n' % (j, qq[j]))


    def run_create_batch(self):

        self.log.info('\n\n'+'-'*50+'\nRunning Create Batch\n'+'-'*50+'\n')
        sys.stdout.write('\n\n'+'-'*50+'\nRunning Create Batch\n'+'-'*50+'\n')

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

        self.log.info('\n\n'+'-'*50+'\nRunning ETS Validation\n'+'-'*50+'\n')
        sys.stdout.write('\n\n'+'-'*50+'\nRunning ETS Validation\n'+'-'*50+'\n')

        self.rev_opts = config.getConfigSectionMap( self.config_vars, 'ets-validation' )
        upcols = self.rev_opts['column_updates'].split('|')
        mpcols = self.rev_opts['column_map'].split('|')
        col_head = self.gapi.column_headers()[0]

        if batch_id == -999:
            if 'batch' in self.batch_create_data.keys():
                batch_id = self.batch_create_data['batch'][1]['batch']
            else:
                sys.log.error('batch id not found, cannot proceed with ETS validation')
                sys.exit(1)


        ## check if batch_id is in the spreadsheet

        range_names = [
                self.gapi.range('batch_id_col')
                ]
        result = self.service.spreadsheets().values().batchGet(
                spreadsheetId = self.gapi.get_id(),
                ranges=range_names,
                majorDimension = 'COLUMNS',
                valueRenderOption = 'UNFORMATTED_VALUE' ).execute()


        update_sheet = True
        if not result.get('valueRanges')[0].has_key(u'values'):
            self.log.error('Could not fetch values in specified range: \'%s\', cannot update spreadsheet with id: \'%s\'' % (range_names[0], self.gapi.get_id()))
            update_sheet = False
        else:
            try:
                row_num = result.get('valueRanges')[0].get('values')[0].index(int(batch_id)) + 1
            except ValueError:
                self.log.error('Could not find batch_id = %d in specified range: %s in spreadsheet with id: \'%s\'' % (batch_id, range_names[0], self.gapi.get_id()))
                update_sheet = False
                pass

        cmd = [ 'sqlplus', '-S', 'apps/apps', '@' + os.path.join(self.sql_path, 'ets_validate.sql'), str(batch_id) ]

        self.log.debug(' '.join(cmd))

        (self.std_out['ets_valid'], self.std_err['ets_valid'], p) =  os_system_command( ' '.join(cmd), self.run_env)
        if not p.returncode == 0:
            self.log.error(self.std_err['ets_valid'])
            sys.exit(p.returncode)
        else:
            self.log.info('\n'+('-'*20)+'\n'+self.std_out['ets_valid'])

        self.ets_codes_found = False
        key = self.rev_opts['ets_key']
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

        update_row = None

        if update_sheet:
            range_names = [ self.rev_opts['data_range'] % (row_num, row_num)]
            result = self.service.spreadsheets().values().batchGet(
                spreadsheetId = self.gapi.get_id(),
                ranges=range_names,
                majorDimension = 'ROWS',
                valueRenderOption = 'UNFORMATTED_VALUE' ).execute()

            if not result.get('valueRanges')[0].has_key(u'values'):
                self.log.error('Could not update row %d for batch_id = %d in spreadsheet with id: \'%s\'' % (row_num, batch_id, range_names[0], self.gapi.get_id()))
            else:
                update_row = result.get(u'valueRanges')[0].get(u'values')[0]

        ets_validate_data = { 'evstatus': self.msg_type[not self.ets_codes_found], 'lvcodes': codes[key] }


        status_row = collections.OrderedDict( zip(col_head, update_row))


        for i in range(0, len(upcols)):
            key = mpcols[i]
            head = upcols[i]
            status_row[head] = ets_validate_data[key]

        data = [
            {
                'range': range_names[0],
                'values': [[ v for k,v in status_row.iteritems() ]]
                },
            ]

        body_update = {
            'valueInputOption': 'USER_ENTERED',
            'data': data
        }

        result = self.service.spreadsheets().values().batchUpdate(
                 spreadsheetId = self.gapi.get_id(), body=body_update).execute()

        msg_body = '\nETS Validation for batch %s: %s %s \n' % ( batch_id , ets_validate_data['evstatus'], ets_validate_data['lvcodes'] )

        self.notifier.message('ev_notify_codes', msg_body)


    def run_request_set(self, batch_id):

        self.rrs_opts = config.getConfigSectionMap( self.config_vars, 'request-set' )
        upcols = self.rrs_opts['column_updates'].split('|')
        mpcols = self.rrs_opts['column_map'].split('|')
        col_head = self.gapi.column_headers()[0]

        self.log.info('\n\n'+'-'*50+'\nRunning Request Set\n'+'-'*50+'\n')
        sys.stdout.write('\n\n'+'-'*50+'\nRunning Request Set\n'+'-'*50+'\n')

        if batch_id == -999:
            if 'batch' in self.batch_create_data.keys():
                batch_id = self.batch_create_data['batch'][1]['batch']
            else:
                sys.log.error('batch id not found, cannot proceed with running request set')
                sys.exit(1)


        ## check if batch_id is in the spreadsheet
        range_names = [
                self.gapi.range('batch_id_col')
                ]
        result = self.service.spreadsheets().values().batchGet(
                spreadsheetId = self.gapi.get_id(),
                ranges=range_names,
                majorDimension = 'COLUMNS',
                valueRenderOption = 'UNFORMATTED_VALUE' ).execute()

        update_sheet = True

        if not result.get('valueRanges')[0].has_key(u'values'):
            self.log.error('Could not fetch values in specified range: \'%s\', cannot update spreadsheet with id: \'%s\'' % (range_names[0], self.gapi.get_id()))
            update_sheet = False
        else:
            try:
                row_num = result.get('valueRanges')[0].get('values')[0].index( int(batch_id) ) + 1
            except ValueError:
                self.log.error('Could not find batch_id = %d in specified range: %s in spreadsheet with id: \'%s\'' % (batch_id, range_names[0], self.gapi.get_id()))
                update_sheet = False


        if self.ets_codes_found == None:
            self.run_ets_validate(batch_id)

        if self.ets_codes_found == True:
            self.log.warn('ETS codes need to be added for batch ID %s' % str(batch_id))
            sys.stderr.write('WARN: ETS codes need to be added for batch ID %s' % str(batch_id))
            sys.exit(1)

        cmd = [ 'sqlplus', '-S', 'apps/apps', '@'+ os.path.join(self.sql_path, 'run_request_set.sql'), str(batch_id) ]

        self.log.debug(' '.join(cmd))
        (self.std_out['request_set'], self.std_err['request_set'] , p) =  os_system_command( ' '.join(cmd), self.run_env)
        if not p.returncode == 0:
            self.log.error(self.std_err['request_set'])
            sys.exit(p.returncode)
        else:
            self.log.info('\n'+('-'*20)+'\n'+self.std_out['request_set'])


        update_row = None
        if update_sheet:
            range_names = [ self.rrs_opts['data_range'] % (row_num, row_num)]

            result = self.service.spreadsheets().values().batchGet(
                spreadsheetId = self.gapi.get_id(),
                ranges=range_names,
                majorDimension = 'ROWS',
                valueRenderOption = 'UNFORMATTED_VALUE' ).execute()

            if not result.get('valueRanges')[0].has_key(u'values'):
                self.log.error('Could not update row %d for batch_id = %d in spreadsheet with id: \'%s\'' % (row_num, batch_id, range_names[0], self.gapi.get_id()))
            else:
                update_row = result.get('valueRanges')[0].get('values')[0]

        key = self.rrs_opts['rqs_key']
        pattern = re.compile( r''.join( [ r'(?P<fmsg>%s[^\d]+)' % key, r'(?P<id>[\d]+)' ]) )
        codes = dict()

        run_requestset_data = { 'rqstatus': '(null)', 'rqsetid': '(null)'}
        for line in self.std_out['request_set'].split('\n'):
            m_grp = pattern.match(line)
            if m_grp:
                meta = m_grp.groupdict()
                run_requestset_data['rqstatus'] = meta['fmsg'].split()[0]
                run_requestset_data['rqsetid'] = meta['id'].strip()
                break

        status_row = collections.OrderedDict( zip(col_head, update_row))


        for i in range(0, len(upcols)):
            key = mpcols[i]
            head = upcols[i]
            status_row[head] = run_requestset_data[key]

        data = [
            {
                'range': range_names[0],
                'values': [[ v for k,v in status_row.iteritems() ]]
                },
            ]

        body_update = {
            'valueInputOption': 'USER_ENTERED',
            'data': data
        }

        result = self.service.spreadsheets().values().batchUpdate(
                 spreadsheetId = self.gapi.get_id(), body=body_update).execute()

        msg_body = '\nRequest Set ID: %s for batch-id:%s %s \n' % ( run_requestset_data['rqsetid'], batch_id, run_requestset_data['rqstatus'] )

        self.notifier.message('rq_notify_status', msg_body)


    def validate_configuration(self):

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

        self.run_env = capture_env(self.load_opts['environment'], { 'CLASSPATH' : self.class_path } )
        if self.options.logLevel == 'VERBOSE':
            out = ''
            for key in self.run_env:
                out += key+'='+self.run_env[key]+'\n'
            self.log.info(out)


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

        status = sorted(self.batch_create_data.keys()) == sorted(required)
        self.batch_create_data.update( { 'cbstatus':[self.msg_type[status], {'cbstatus':self.msg_type[status]} ]})


    def update_createbatch(self, append = True):

        self.bcs_opts = config.getConfigSectionMap( self.config_vars, 'create-batch' )
        upcols = self.bcs_opts['column_updates'].split('|')
        mpcols = self.bcs_opts['column_map'].split('|')

        col_head = self.gapi.column_headers()[0]

        status_row = collections.OrderedDict()
        for i in col_head:
            status_row[i] = '(null)'

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
                 range = self.gapi.range('column_range'), insertDataOption='INSERT_ROWS',
                 body=body_append).execute()

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
        if self.linkdir:
            try:
                os.unlink( self.linkdir )
            except OSError:
                self.log.error('cannot remove file/link: %s' % self.linkdir)
                sys.exit(1)


    def run(self):
        pars = [ 'user = %s' % self.user, 'datatype = %s' % self.load_opts['category'], 'pid = %s' % self.pid, datetime.datetime.now().strftime("last_run = %F %H:%M:%S\n") ]

        lock = Lock(self.lockfile, pars)
        try:
            ## get exclusive lock
            lock.acquire()

            ## validate input
            self.validate_configuration()

            ## path one is creating a new batch(man), ets-validate(opt) and run-request set(opt)
            if self.opt_path == 'one':
                ## create batch
                self.run_create_batch()

                ## update spreadsheet
                self.update_createbatch()

                ## validate ETS codes
                if self.load_opts['bc_ets_validate'].upper()[0] == 'Y':
                    self.run_ets_validate(-999)

                time.sleep( int(self.load_opts['delay']))
                ## run request set
                if self.load_opts['bc_run_requestset'].upper()[0] == 'Y':
                    self.run_request_set(-999)

            ## path two is ets-validate(opt) and run-request set(opt) with an existing batch id
            elif self.opt_path == 'two':

                ## validate ETS codes
                if self.options.ets_validate:
                    self.run_ets_validate(self.options.batch_id)

                time.sleep( int(self.load_opts['delay']))
                ## run request set
                if self.options.run_request:
                    self.run_request_set(self.options.batch_id)
        except:
            raise
        finally:
            self.cleanup()
            lock.release()
            self.log.info('Processing complete')

