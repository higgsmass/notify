import os
import re
import sys
import csv
import time
import glob
import stat
import fnmatch
import logging
import datetime
import subprocess

## local imports
import config
import logger



def os_system_command(cmd, m_env=None):
    res = None
    out = None
    err = None
    try:
        if m_env == None:
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env = m_env)
        out, err = p.communicate()
        yield (out, err, p)
        p.wait()
    except OSError:
        sys.exit(p.returncode)




class loader(object):

    def __init__(self, options):

        self.options = options
        self.config_path = None
        self.config_vars = None

        self.sym_link = ''
        self.class_path = '.:'
        try:
            self.class_path += os.environ['CLASSPATH'] + ':'
        except KeyError:
            pass
        self.valid_sites = []
        self.std_out = None
        self.std_err = None

        ## parse log-file and extract parameters -- begin vars
        self.parse_lines = None
        #self.pat_common = [ r'(?P<time>.+)', r'\[(?P<process>\S+)\]', r'(?P<status>Line: [0-9]+\S+%s .*\-)' % self.load_opts['log_level'], r'(?P<message>.*)' ]
        #self.pat_fir = [ r'(?P<time>.+)', r'\[(?P<process>\S+)\]', r'(?P<status>Line: [0-9]+\S+INFO.*\-)', r'(?P<message>\S+Processing Errors.*)' ]
        ## parse log-file and extract parameters -- end vars

        sys.stderr.write("\n--> Reading configuration file: %s\n" % options.conf)
        try:
            with open (options.conf, 'r') as f:
                f.close()
            self.config_path = options.conf
            self.config_vars = config.getConfigParser(self.config_path)
        except IOError, err:
            sys.stderr.write('ERROR: %s\n' % str(err))
            traceback.print_exc()
            sys.exit(err.errno)

        self.load_opts = config.getConfigSectionMap( self.config_vars, "bulk_load" )
        self.log = logger.logInit(self.options.logLevel, self.load_opts['log_path'], type(self).__name__)

        if self.options.show:
            self.printconfig()


    def printconfig(self):
        sec_done = []
        for item in self.config_vars.sections():
            qq = config.getConfigSectionMap(self.config_vars, item)
            sys.stderr.write('\n----------------------[ %s ]----------------------\n' % item)
            for j in qq:
                if j not in sec_done:
                    sys.stderr.write( '%-25s: %s\n' % (j, qq[j]))
                    sec_done.append(j)


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
        for (self.std_out, err, p) in os_system_command( ' '.join(cmd), self.run_env):
            pass
        if not p.returncode == 0:
            self.log.error(err)
            sys.exit(p.returncode)
        self.get_batch_status()


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
        linkdir = os.path.join( os.getcwd(), linkname)
        self.sym_link = os.path.join(self.load_opts['basepath'] , linkname)
        link_exists = False
        try:
            link_exists = stat.S_ISLNK(os.lstat(linkdir).st_mode)
        except OSError:
            pass
        if link_exists:
            self.log.warn('symbolic link %s exists. trying to remove it' % linkdir)
            try:
                os.unlink( linkdir )
            except OSError:
                self.log.error('cannot remove file/link: %s' % linkdir)
                sys.exit(1)
        try:
            if self.options.logLevel == 'VERBOSE':
                self.log.info('creating symbolic link %s -> %s ' % (linkdir, self.sym_link))
            os.symlink( self.sym_link, linkdir )
        except OSError:
            self.log.error('cannot symlink: %s -> %s' % (linkdir, self.sym_link))

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
            self.log.error( 'I/O error {0}" {1}'.format(e.errno, e.strerror))
            sys.exit(e.errno)

        ## check if site is one among allowed (valid) sites
        if self.options.blsite.strip() not in self.valid_sites:
            self.log.error( 'not a valid site: %s. sites must be one of [ %s ]' % (self.options.blsite, ', '.join(self.valid_sites)) )
            sys.exit(1)

        ## ensure batch size > 0
        if self.options.blsize < 1:
            self.log.error('invalid batch size: %d, must be a positive whole number' % self.options.blsize)
            sys.exit(1)

        ## append additional environment (capture default env set by ORACLE apps)
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


    def get_batch_status(self):
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

        extracts = dict()

        for line in self.std_out.split('\n'):
            for pat in patterns:
                multiline = True in patterns[pat]
                res = None
                mat_grp = patterns[pat][0].match(line)
                if mat_grp:
                    res = mat_grp.groupdict()
                if res:
                    if pat in extracts and multiline:
                        ## assume integer addition and try to add the two
                        self.log.warn('already exists: %s' % line)
                        try:
                            res[pat] = str( int(extracts[pat][1][pat]) + int(res[pat]) )
                        except ValueError:
                            pass
                    else:
                        extracts.update( { pat:[line, res]})
        for i in extracts:
            print i, extracts[i][1][i]

    def match_pattern(self):
        pat1 = re.compile(r'\s+'.join(self.pat_common))
        try:
            with open(self.load_opts['bl_logpath']) as f:
                self.parse_lines = itertools.islice(f, self.init_line, None)
                for line in self.parse_lines:
                    mat_grp = pat1.match(line)
                    if mat_grp:
                        res = mat_grp.groupdict()
                        print res
                    else:
                        res = re.findall(r'\s+'.join(self.pat_extra), line, re.MULTILINE)
                        print res

        except IOError as err:
            self.log.error( 'I/O error {0}" {1}'.format(e.errno, e.strerror))


    def run(self):
        self.printconfig()
        self.validate()
        self.execute()
        self.log.info('Processing complete')

