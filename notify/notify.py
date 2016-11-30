import os
import re
import sys
import time
import twilio
import select
import httplib
import requests
import termcolor

## local imports
import config
import logger
import helper

class notify(object):

    def __init__(self, parser, cfgdb, cfuser, cfgvars, log = None):

        self.ftu_message = termcolor.colored ( "\033[1m" + '''
___________________________________________________________________________________

                   F I R S T     T I M E    U S E R
                  ------------  ---------  ---------

User "%s" with phone "%s" not found in database.

Adding user and authenticating the phone number.
When you receive the message, enter the verification code below to proceed

Sending a verification code on your device
___________________________________________________________________________________

''' + "\033[0;0m" , 'cyan')

        self.parser = parser
        self.config_vars = cfgvars
        self.db = cfgdb
        self.user = cfuser
        self.heading = "notify"
        self.client = None
        self.options = parser.parse_args()
        self.incidents = dict()
        self.messages = True
        self.phone_verified = False
        self.vcode = None
        self.tel = { 'from_number': None, 'to_number': '+1', 'to_split': None }

        self.conf_opts = config.getConfigSectionMap( self.config_vars, self.heading )

        inc_pattern = re.compile('.*_notify_.*')
        [ self.incidents.update({ key:inc_pattern.match(key)}) for key in self.conf_opts.keys() if '_notify_' in key  ]

        if not log:
            self.log = logger.logInit(self.options.logLevel, self.conf_opts['log_path'], type(self).__name__)
        else:
            self.log = log

        if self.options.show:
            self.printconfig()

        ## re for grabbing area-code / trunk / last four of 10 digit phone number
        tel_reg = [ r'(?P<acode>(\d{3}))', r'(?P<trunk>(\d{3}))', r'(?P<lfour>(\d{4}))' ]
        tel_pat = re.compile('\D*'.join(tel_reg))
        phno = tel_pat.match(self.conf_opts['phone_number'])


        ## can't identify phone number implies, no messaging
        if not phno:
            self.log.error('Cannot parse/understand the phone you number provided: %s' % self.conf_opts['phone_number'])
            self.messages = False
        else:
            self.tel['to_split'] = phno.groupdict()
            self.tel['to_number'] += (self.tel['to_split']['acode'] + self.tel['to_split']['trunk'] + self.tel['to_split']['lfour'])

        sql = 'SELECT account_sid, auth_token from twilio;'
        self.auth = self.db.fetchdict(sql)

        sql = 'SELECT from_number from twilio;'
        self.tel.update(self.db.fetchdict(sql))

        self.meta = self.db.fetchdict('SELECT * from authy;')

        ## verify first time user's phone number using authy
        sql = 'select * from authuser where uname = \'%s\' and phone like \'%s\';' % (self.user, self.tel['to_number'])
        udet = self.db.fetchdict(sql)
        if udet['id'] == None and udet['verified'] == None:
            msg = self.ftu_message % (self.user, self.tel['to_number'])
            sys.stdout.write(msg)
            self.verify_phone()
            if self.phone_verified:
                sql = 'INSERT INTO authuser (uname,phone,verified) values(\'%s\', \'%s\', 1);' % (self.user, self.tel['to_number'])
                self.db.cursor().execute(sql)
        else:
            self.phone_verified = udet['verified'] == 1
            if self.phone_verified:
                self.messages = True
        try:
            self.client = twilio.rest.TwilioRestClient( self.auth['account_sid'] , self.auth['auth_token'] )
        except twilio.TwilioRestException as err:
            sys.stderr.write('ERROR: %s\n' % str(err))


    def verify_phone(self):
        phone = '-'.join( [ self.tel['to_split']['acode'], self.tel['to_split']['trunk'], self.tel['to_split']['lfour'] ])
        headers = { 'X-Authy-API-Key': self.meta['authy_key'] }
        payload = { 'via':'sms', 'country_code':'1', 'phone_number': phone }
        r = requests.post(self.meta['post_url'], data = payload, headers = headers)
        if not r.status_code == httplib.OK:
            sys.stderr.write('WARN: Failed to send verify phone number %s' % phone)
        else:
            response = r.json()
            sys.stdout.write('\n' + response['message'] + '\n\n')
            time.sleep(5)
            self.vcode = countdown('Enter verification code: ', response['seconds_to_expire'])
            ## can't verify phone for first time user. no messaging
            if not self.vcode:
                self.messages = False
            payload = { 'phone_number':phone, 'country_code':'1', 'verification_code':self.vcode }
            r = requests.get( self.meta['get_url'], data = payload, headers = headers)
            if not r.status_code == httplib.OK:
                sys.stderr.write( termcolor.colored('WARN: Failed to verify phone number %s' % phone, 'red'))
            else:
                response = r.json()
                msg = 'SUCCESS: ' + response['message']
                sys.stdout.write ( termcolor.colored( "\033[1m" + msg + "\033[0;0m" , 'green') )
            self.phone_verified = True

    def printconfig(self):
        sys.stderr.write('\n----------------------[ %s ]----------------------\n' % self.heading)
        qq = config.getConfigSectionMap(self.config_vars, self.heading)
        for j in qq:
            if j not in self.config_vars.defaults().keys():
                sys.stderr.write( '%-25s: %s\n' % (j, qq[j]))


    def message(self, msg_header, msg_body):

        if msg_header in self.incidents.keys() and self.incidents[msg_header] != None and self.messages:
            try:
                message = self.client.messages.create( body= msg_body, to = str(self.tel['to_number']), from_ = self.tel['from_number'])
                self.log.info('%s: message sent to recipient, body = \"%s\"' % (msg_header, msg_body))
            except twilio.TwilioRestException as err:
                self.log.error(msg_body)
                self.log.error(err)

