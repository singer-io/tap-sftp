#!/usr/bin/env python3

# This file was intended to be run with:
# $ python -i connect.py


from ftplib import FTP_TLS
import ssl


# Attempt to make a SSL Context object where we disable certificate verification
# Maybe works? Could not get FTPS server working.
ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
ctx.options |= ssl.OP_NO_SSLv3
ctx.verify_mode = ssl.CERT_NONE

HOST = '172.17.0.2'
PORT = 21
USER = 'ftptest'
PASS = 'ftptest'


def connect():
    ftp = FTP_TLS(host=HOST, context=ctx)
    ftp.login(user=USER, passwd=PASS)
    return ftp


ftp = connect()
ftp.set_debuglevel(2)
ftp.prot_p()
