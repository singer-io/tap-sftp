from ftplib import FTP_TLS
from ftplib import FTP
import ssl


# Instantiate a ftps client object with our context above

HOST = '172.17.0.2'
PORT = 21
USER = 'ftptest'
PASS = 'ftptest'

ALL_CMDS = ['!', 'dir', 'mdelete', 'qc', 'site', '$', 'disconnect',
            'mdir', 'sendport', 'size', 'account', 'exit', 'mget', 'put', 'status',
            'append', 'form', 'mkdir', 'pwd', 'struct', 'ascii', 'get', 'mls', 'quit',
            'system', 'bell', 'glob', 'mode', 'quote', 'sunique', 'binary', 'hash',
            'modtime', 'recv', 'tenex', 'bye', 'help', 'mput', 'reget', 'tick',
            'case', 'idle', 'newer', 'rstatus', 'trace', 'cd', 'image', 'nmap',
            'rhelp', 'type', 'cdup', 'ipany', 'nlist', 'rename', 'user', 'ch mod',
            'ipv4', 'ntrans', 'reset', 'umask', 'close', 'ipv6', 'open', 'restart',
            'verbose', 'cr', 'lcd', 'prompt', 'rmdir', '?', 'delete', 'ls', 'passive',
            'runique', 'debug', 'macdef', 'proxy', 'send']



def connect():
    ftp = FTP_TLS(host=HOST)
    ftp.login(user=USER, passwd=PASS)
    return ftp


# Verbose debug information for testing the implementation 

ftp = connect()
ftp.set_debuglevel(2) 

# try:
#     ftp.dir()
# except Exception as err:
#     import ipdb; ipdb.set_trace()
#     1+1

#ftp.prot_p()
# import ipdb; ipdb.set_trace()
# 1+1
#ftp.retrlines('LIST')
