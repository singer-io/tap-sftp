#!/usr/bin/env python3

# This file was intended to be run with:
# $ python -i connect.py


from ftplib import FTP_TLS


HOST = '172.17.0.2'
PORT = 21
USER = 'ftptest'
PASS = 'ftptest'


def connect():
    ftp = FTP_TLS(host=HOST)
    ftp.login(user=USER, passwd=PASS)
    return ftp


ftp = connect()
ftp.prot_p()
