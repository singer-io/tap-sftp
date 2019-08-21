#!/usr/bin/env python3
from ftplib import FTP
import paramiko


CONNECTION_HOST = '172.17.0.1'
CONNECTION_PORT = 2222
CONNECTION_PASS = 'pass'
CONNECTION_USER = 'foo'

def connect():
    transport = paramiko.Transport((CONNECTION_HOST, CONNECTION_PORT))
    transport.connect(username=CONNECTION_USER, password=CONNECTION_PASS)
    ftp = paramiko.SFTPClient.from_transport(transport)
    return ftp


ftp = connect()

# This needs to happen based on the way set up the FTP server
ftp.chdir('upload')

data_file = ftp.open('test_data.csv', mode='r', bufsize=-1)

# We expect this to be the number of lines in the csv file
# And we assume that the first column of the row is an `id`
print([x.split(',')[0] for x in data_file.readlines()])

ftp.close()
