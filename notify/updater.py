import os
import re
import sys
import pwd
import csv
import time
import stat
import fcntl

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

from helper import *

class updater(object):

    def __init__(self, parser):

        self.parser = parser
        self.heading = "bulk_load"
        self.options = parser.parse_args(sys.argv[1:])
        self.config_path = None
        self.config_vars = None
        self.pid = None
        self.lockfile = None
        self.upd_tids = None
        self.class_path = '.:'
        self.run_env = None
        self.msg_type = { True:'SUCCESS', False:'PROCESSED' }
        try:
            self.class_path += os.environ['CLASSPATH'] + ':'
        except KeyError:
            pass
        self.user = os.environ['USER']


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


        msg_head = 'Command line Options:\n'

        for arg, value in sorted(vars(self.options).items()):
            msg_head += '%s = %r\n' % (arg, value)

        self.log.info(msg_head)

        ## validate user
        current_user = self.user.upper().strip()
        config_user = self.load_opts['user'].upper().strip()
        if current_user == config_user:
            msg = 'Running as user %s\n' % self.user
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


    def printconfig(self, defaults):
        if defaults:
            sys.stderr.write('\n----------------------[ DEFAULT ]----------------------\n')
            for k,v in self.config_vars.defaults().iteritems():
                sys.stderr.write( '%-25s: %s\n' % (k, v))

        qq = config.getConfigSectionMap(self.config_vars, self.heading)
        sys.stderr.write('\n----------------------[ %s ]----------------------\n' % self.heading)
        for j in qq:
                sys.stderr.write( '%-25s: %s\n' % (j, qq[j]))


    def run_get_status(self):

        self.log.info('\n\n'+'-'*50+'\nRunning GET STATUS\n'+'-'*50+'\n')
        sys.stdout.write('\n\n'+'-'*50+'\nRunning GET STATUS\n'+'-'*50+'\n')

        self.rev_opts = config.getConfigSectionMap( self.config_vars, 'update-status' )
        upcols = self.rev_opts['column_updates'].split('|')
        mpcols = self.rev_opts['column_map'].split('|')
        col_head = self.gapi.column_headers()[0]


        ## check if request set id is in the spreadsheet

        range_names = [
                self.gapi.range('reqset_id_col'),
                self.gapi.range('reqset_status_col')
                ]
        result = self.service.spreadsheets().values().batchGet(
                spreadsheetId = self.gapi.get_id(),
                ranges=range_names,
                majorDimension = 'COLUMNS',
                valueRenderOption = 'UNFORMATTED_VALUE' ).execute()

        ## get all the rows for columns = request_id, request_status
        update_sheet = True
        if not result.get('valueRanges')[0].has_key(u'values'):
            self.log.error('Could not fetch values in specified range: \'%s\', cannot update spreadsheet with id: \'%s\'' % (range_names[0], self.gapi.get_id()))
            update_sheet = False
        else:
            try:
                self.upd_tids = collections.OrderedDict((k,v) for k,v in zip( result.get('valueRanges')[0].get('values')[0], result.get('valueRanges')[1].get('values')[0] ) if v == self.rev_opts['pick_status'])
            except ValueError:
                self.log.error('Could not find batch_id = %d in specified range: %s in spreadsheet with id: \'%s\'' % (batch_id, range_names[0], self.gapi.get_id()))
                update_sheet = False
                pass

        ## initialize row for updating spreadsheet
        #status_row = collections.OrderedDict()
        #for i in col_head:
        #    status_row[i] = '(null)'

        ## criteria to parse and extract info from output of get_status query
        l_common = [ r'(?P<vbstatus>[A-Z])\,', r'(?P<fmsg1>START TIME:)', r'(?P<vbstime>[0-9\/\:\ ]+)\,', r'(?P<fmsg2>COMPLETION TIME:)', r'(?P<vbetime>[0-9\/\:\ ]+)' ]
        l_vb = [ r'(?P<fmsg0>.*Bulk Load Validate Batch.*STATUS:)' ] + l_common
        l_tb = [ r'(?P<fmsg0>.*Bulk Load Transfer Batch.*STATUS:)' ] + l_common
        pat_vb = re.compile( '\s+'.join (l_vb) )
        pat_tb = re.compile( '\s+'.join (l_tb).replace('vb', 'tb') )
        req_fields = [ 'vbstatus', 'vbstime', 'vbetime', 'tbstatus', 'tbstime', 'tbetime' ]


        if update_sheet:
            ## capture environment to run sql query
            self.run_env = capture_env(self.load_opts['environment'], { 'CLASSPATH' : self.class_path } )

            ## loop over all rows that need update
            for tid in self.upd_tids:

                ## run command (get_status)
                cmd = [ 'sqlplus', '-S', 'apps/apps', '@'+ os.path.join(self.sql_path, 'get_status.sql'), str(tid) ]
                self.log.debug(' '.join(cmd))
                self.log.info('Processing Request Set ID: %s' % tid)
                (out, err, p) =  os_system_command( ' '.join(cmd), self.run_env)
                update_tidrow = False
                if not p.returncode == 0:
                    self.log.error(err)
                    continue
                else:
                    ## parse and get data to update
                    upd_defaults = { 'rqstatus':'PROCFAIL', 'vbstatus':'(null)', 'vbstime':'(null)',
                            'vbetime':'(null)', 'tbstatus':'(null)', 'tbstime':'(null)', 'tbetime':'(null)' }

                    for line in out.split('\n'):
                        grp_vb = pat_vb.match(line)
                        if grp_vb:
                            m_vb = grp_vb.groupdict()
                            for key in m_vb.keys():
                                if key in req_fields:
                                    upd_defaults.update( { key:m_vb[key] } )
                        grp_tb = pat_tb.match(line)
                        if grp_tb:
                            m_tb = grp_tb.groupdict()
                            for key in m_tb.keys():
                                if key in req_fields:
                                    upd_defaults.update( { key:m_tb[key] } )

                    upd_defaults['rqstatus'] = self.msg_type [ upd_defaults['vbstatus'] == 'C' and upd_defaults['tbstatus'] == 'C' ]


                    ## first get row for this tid
                    try:
                        row_num = result.get('valueRanges')[0].get('values')[0].index(int(tid)) + 1
                        update_tidrow = True
                    except ValueError:
                        self.log.error('Could not find batch_id = %d in specified range: %s in spreadsheet with id: \'%s\'' % (tid, range_names[0], self.gapi.get_id()))
                        pass

                    ## parse and get data to update
                    if update_tidrow:
                        range_names = [ self.rev_opts['data_range'] % (row_num, row_num)]
                        res_tid = self.service.spreadsheets().values().batchGet(
                            spreadsheetId = self.gapi.get_id(),
                            ranges=range_names,
                            majorDimension = 'ROWS',
                            valueRenderOption = 'UNFORMATTED_VALUE' ).execute()

                        if not res_tid.get('valueRanges')[0].has_key(u'values'):
                            self.log.error('Could not update row %d for batch_id = %d in spreadsheet with id: \'%s\'' % (row_num, tid, range_names[0], self.gapi.get_id()))
                        else:
                            update_row = res_tid.get(u'valueRanges')[0].get(u'values')[0]
                            status_row = collections.OrderedDict( zip(col_head, update_row))
                        ## prepare update row
                        for i in range(0, len(upcols)):
                            status_row[ upcols[i] ] = upd_defaults[ mpcols[i] ]

                        ## update row
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

                        res_upd = self.service.spreadsheets().values().batchUpdate(
                                spreadsheetId = self.gapi.get_id(),
                                body = body_update).execute()


                    self.log.info('\n'+('-'*20)+'\n'+out)



    def run(self):
        pars = [ 'user = %s' % self.user, 'datatype = %s' % self.load_opts['category'], 'pid = %s' % self.pid, datetime.datetime.now().strftime("last_run = %F %H:%M:%S\n") ]

        lock = Lock(self.lockfile, pars)
        try:
            ## get exclusive lock
            lock.acquire()

            ## get status
            self.run_get_status()

        except:
            raise
        finally:
            lock.release()
            self.log.info('Processing complete')

