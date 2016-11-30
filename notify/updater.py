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
        self.sym_link = ''
        self.class_path = '.:'
        self.run_env = None
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


        ## check if batch_id is in the spreadsheet

        range_names = [
                self.gapi.range('reqset_id_col'),
                self.gapi.range('reqset_status_col')
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
                self.upd_tids = collections.OrderedDict((k,v) for k,v in zip( result.get('valueRanges')[0].get('values')[0], result.get('valueRanges')[1].get('values')[0] ) if v == self.rev_opts['pick_status'])
            except ValueError:
                self.log.error('Could not find batch_id = %d in specified range: %s in spreadsheet with id: \'%s\'' % (batch_id, range_names[0], self.gapi.get_id()))
                update_sheet = False
                pass

        if update_sheet:
            self.run_env = capture_env(self.load_opts['environment'], { 'CLASSPATH' : self.class_path } )
            for tid in self.upd_tids:
                cmd = [ 'sqlplus', '-S', 'apps/apps', '@'+ os.path.join(self.sql_path, 'get_status.sql'), str(tid) ]
                self.log.debug(' '.join(cmd))
                self.log.info('Processing Request Set ID: %s' % tid)
                sys.stdin.read(1)
                (out, err, p) =  os_system_command( ' '.join(cmd), self.run_env)
                if not p.returncode == 0:
                    self.log.error(err)
                    continue
                else:
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

