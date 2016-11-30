import os
import sys
import fcntl
import subprocess

import fnmatch
import sqlite3
import select
import traceback


## running a system command within an env
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

## capture environment by sourcing a script
## optionally append additional variables if
def capture_env(env_var_script, append_vars):

    env_vars = None
    ## append additional environment (capture default env set by ORACLE apps)
    try:
        p = subprocess.Popen( ['bash', '-c', "trap 'env' exit; source \"$1\" > /dev/null 2>&1",
            "_", env_var_script ], shell=False, stdout=subprocess.PIPE)
        (env_vars, cerr) = p.communicate()
        if not p.returncode == 0:
            sys.stderr.write(cerr)
    except OSError:
        pass

    if env_vars == None:
        return env_vars

    ## base env
    run_env = dict(os.environ)

    ## oracle env
    env_var_status = [ (key,False) for key in sorted(append_vars.keys()) ]
    for line in env_vars.split('\n'):
        if not '=' in line:
            continue
        try:
            key, val = line.split('=', 1)

            ## append env-var if key is foound
            for ikey in env_var_status:
                if key == ikey[0]:
                    val += ':' + append_vars[ikey[0]]
                    l = list(ikey)
                    l[1] = True
                    ikey = tuple(l)
                run_env.update({ key:val })

            ## if key is not found, add new key/var to env
            for ikey in env_var_status:
                if not ikey[1]:
                    run_env.update( { ikey[0]:append_vars[ ikey[0]] })
        except ValueError, err:
            pass

    return run_env


## wait for user input
def userinput(timeout=0.0):
    return select.select([sys.stdin], [], [], timeout)[0]


## countdown timer until user input is received
def countdown(msg, tmax):
    decr = 1 ## refresh time decrement
    while tmax >= 0:
        show = '\r' + msg + '[ %3d s remaining ]: ' % tmax
        sys.stdout.write(show)
        sys.stdout.flush()
        tmax -= decr
        if userinput(decr):
            return raw_input()
    return None


## lightweight db object to store sensitive notification (or other data)
class SQLiteDB:

    def __init__(self, path):

        self.path = path
        self.db = None

        if not os.path.exists(os.path.split(self.path)[0]):
            sys.stderr.write('ERROR: Unknown path/filename: %s' % self.path)
            sys.exit(1)
        try:
            self.db = sqlite3.connect(self.path)
        except IOError as e:
            sys.stderr.write(str(e))
            sys.stderr.write('ERROR: Creating/opening dataase: %s' % self.path)
            sys.exit(1)

    def fetchdict(self, sql, params=()):
        cur = self.db.cursor().execute(sql, params)
        res = cur.fetchone()
        if res is None:
            return { k[0]:None for k in cur.description }
        return { k[0]: v for k, v in list(zip(cur.description, res)) }


    def handle(self):
        return self.db

    def cursor(self):
        return self.db.cursor()

    def __del__(self):
        if self.db:
            self.db.commit()
            self.db.close()


## acquire/release exclusive lock for a program
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
        if self.handle:
            self.handle.close()
            try:
                os.remove(self.filename)
            except OSError:
                raise



