#!/usr/bin/env python3
import paramiko


HOST = '172.17.0.2'
PORT = 22
PASS = 'pass'
USER = 'foo'

def connect(host=HOST, port=PORT, user=USER, password=PASS):
    transport = paramiko.Transport((host, port))
    transport.connect(username=user, password=password)
    ftp = paramiko.SFTPClient.from_transport(transport)
    return ftp


def connect_with_key(host=HOST, port=PORT, user=USER, password=PASS):
    our_pkey = paramiko.RSAKey.from_private_key_file('/home/vagrant/.ssh/id_rsa', '')
    transport = paramiko.Transport((host, port))
    transport.connect(username=user, password=password , pkey=our_pkey)
    return paramiko.SFTPClient.from_transport(transport)


def test_read(ftp):
    # This needs to happen based on the way set up the FTP server
    ftp.chdir('upload')

    data_file = ftp.open('test_data.csv', mode='r', bufsize=-1)

    # We expect this to be the number of lines in the csv file
    # And we assume that the first column of the row is an `id`
    print([x.split(',')[0] for x in data_file.readlines()])

    #ftp.close()


#ftp = connect_with_key()
