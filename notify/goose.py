import os
import sys
import logging
import argparse
import httplib2
import apiclient
import oauth2client
from apiclient import discovery
#from oauth2client import client
#from oauth2client import tools

## local imports
import logger
import config


class sheets(object):

    def __init__(self, parser, cfgvars, log = None):

        self.parser = parser
        self.config_vars = cfgvars
        self.heading = "spreadsheet_access"
        self.options = parser.parse_args()
        self.sid = None
        self.http = None
        self.service = None

        ## this is needed for bypassing arguments that are not required by oauth2client.tools argparser which is a PITA
        ## filter out all args except oauth2 args from command line and pass the flags to authentication module
        self.oa2args = dict()

        ## instantiate an oauth2 parser (temporarily)
        par_oauth  = argparse.ArgumentParser(parents=[oauth2client.tools.argparser])

        ## grab all available options for oauth2 parser (except help)
        opt_list = [ vars(action)['option_strings'][0] for action in par_oauth._actions if vars(action)['option_strings'][0] != '-h' ]

        ## if any of our cmdline args match the ones needed by oauth2, save them
        [ self.oa2args.update({k:v}) for k, v in self.options.__dict__.iteritems() if '--'+k in opt_list ]

        ## create a set of flags oauth2 module needs
        self.flags = argparse.Namespace(**self.oa2args)


        self.conf_opts = config.getConfigSectionMap( self.config_vars, self.heading )

        if not log:
            self.log = logger.logInit(self.options.logLevel, self.conf_opts['log_path'], type(self).__name__)
        else:
            self.log = log

        if self.options.show:
            self.printconfig()

    def printconfig(self):
        sys.stderr.write('\n----------------------[ %s ]----------------------\n' % self.heading)
        qq = config.getConfigSectionMap(self.config_vars, self.heading)
        for j in qq:
            if j not in self.config_vars.defaults().keys():
                sys.stderr.write( '%-25s: %s\n' % (j, qq[j]))
        if self.oa2args:
            for j in self.oa2args:
                sys.stderr.write( '%-25s: %s\n' % (j, self.oa2args[j]))

    def get_credentials(self):

        if not os.path.exists(self.conf_opts['credential_path']):
            self.log.warn('%s: credentials directory does not exist, trying to create it' % self.conf_opts['credential_path'])
            try:
                os.makedirs(self.conf_opts['credential_path'])
            except IOError, err:
                sys.stderr.write('ERROR: %s\n' % str(err))
                traceback.print_exc()
                sys.exit(err.errno)

        self.cred_path = os.path.join(self.conf_opts['credential_path'], self.conf_opts['credential_cache'])


        self.store = oauth2client.file.Storage(self.cred_path )
        self.credentials = self.store.get()


        if not self.credentials or self.credentials.invalid:
            self.log.warn('invalid credentials for oauth 2.0 authorization. Trying client secret')
            if not os.path.exists( self.conf_opts['client_secret_path'] ):
                self.log.error('%s: cannot find client secret file' % self.conf_opts['client_secret_path'] )

            self.flow = oauth2client.client.flow_from_clientsecrets(self.conf_opts['client_secret_path'],
                    self.conf_opts['client_scopes'],
                    message = oauth2client.tools.message_if_missing(self.conf_opts['client_secret_path'])
                    )
            self.flow.user_agent = self.conf_opts['client_app_id']
            self.credentials = oauth2client.tools.run_flow(self.flow, self.store, self.flags, http = None)
            self.log.info('storing credentials to %s' % self.conf_opts['credential_path'])

    def get_id(self):
        return self.sid

    def set_id(self, id = None):
        if not id:
            self.sid = self.conf_opts['spreadsheet_id']
        else:
            self.sid = id

    def column_headers(self):
        result = self.service.spreadsheets().values().get(
                spreadsheetId = self.get_id(), range = self.conf_opts['column_range']).execute()
        row = result.get('values')
        return row

    def batch_id_range(self):
        return self.conf_opts['batch_id_col']

    def column_range(self):
        return self.conf_opts['column_range']

    def service_start(self):
        self.get_credentials()
        self.http = self.credentials.authorize(httplib2.Http())
        self.set_id()
        self.service = apiclient.discovery.build('sheets', 'v4', http = self.http, discoveryServiceUrl = self.conf_opts['discovery_url'])
        return self.service


