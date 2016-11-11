import os
import re
import sys
import twilio

## local imports
import config
import logger


class notify(object):

    def __init__(self, parser, cfgvars, log = None):

        self.parser = parser
        self.config_vars = cfgvars
        self.heading = "notify"
        self.client = None
        self.options = parser.parse_args()
        self.incidents = dict()

        self.conf_opts = config.getConfigSectionMap( self.config_vars, self.heading )

        inc_pattern = re.compile('.*_notify')
        [ self.incidents.update({ key:inc_pattern.match(key)}) for key in self.conf_opts.keys() if '_notify' in key  ]

        if not log:
            self.log = logger.logInit(self.options.logLevel, self.conf_opts['log_path'], type(self).__name__)
        else:
            self.log = log

        if self.options.show:
            self.printconfig()

        try:
            self.client = twilio.rest.TwilioRestClient( self.conf_opts['account_sid'] , self.conf_opts['auth_token'] )
        except twilio.TwilioRestException as err:
            sys.stderr.write('ERROR: %s\n' % str(err))

    def printconfig(self):
        sys.stderr.write('\n----------------------[ %s ]----------------------\n' % self.heading)
        qq = config.getConfigSectionMap(self.config_vars, self.heading)
        for j in qq:
            if j not in self.config_vars.defaults().keys():
                sys.stderr.write( '%-25s: %s\n' % (j, qq[j]))
        for j in self.oa2args:
            sys.stderr.write( '%-25s: %s\n' % (j, self.oa2args[j]))


    def message(self, msg_header, msg_body):

        if msg_header in self.incidents.keys() and self.incidents[msg_header] != None:
            try:
                message = self.client.messages.create( body= msg_body, to = str(self.conf_opts['phone_number']), from_='+18037537244')
                self.log.info('%s: message sent to recipient, body = \"%s\"' % (msg_header, msg_body))
            except twilio.TwilioRestException as err:
                self.log.error(err)

