import io
import os
import backoff
import paramiko
import pytz
import re
import singer
import stat
import time
from datetime import datetime
from paramiko.ssh_exception import AuthenticationException

LOGGER = singer.get_logger()

class SFTPConnection():
    def __init__(self, host, username, password=None, private_key_file=None, port=None):
        self.host = host
        self.username = username
        self.password = password
        self.port = int(port)or 22
        self.__active_connection = False
        self.key = None
        if private_key_file:
            key_path = os.path.expanduser(private_key_file)
            self.key = paramiko.RSAKey.from_private_key_file(key_path)

    def handle_backoff(details):
        LOGGER.warn("SSH Connection closed unexpectedly. Waiting {wait} seconds and retrying...".format(**details))

    # If connection is snapped during connect flow, retry up to a
    # minute for SSH connection to succeed. 2^6 + 2^5 + ...
    @backoff.on_exception(backoff.expo,
                          (EOFError),
                          max_tries=6,
                          on_backoff=handle_backoff,
                          jitter=None,
                          factor=2)
    def __try_connect(self):
        try:
            self.transport = paramiko.Transport((self.host, self.port))
            self.transport.use_compression(True)
            self.transport.connect(username = self.username, password = self.password, hostkey = None, pkey = self.key)
        except AuthenticationException as ex:
            self.transport.close()
            self.transport = paramiko.Transport((self.host, self.port))
            self.transport.use_compression(True)
            self.transport.connect(username= self.username, password = self.password, hostkey = None, pkey = None)

    def __ensure_connection(self):
        if not self.__active_connection:
            try:
                self.__try_connect()
            except AuthenticationException as ex:
                raise Exception("Message from SFTP server: {} - Please ensure that the credentials are valid.".format(ex)) from ex
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            self.__active_connection = True

    @property
    def sftp(self):
        self.__ensure_connection()
        return self.__sftp

    @sftp.setter
    def sftp(self, sftp):
        self.__sftp = sftp

    def __enter__(self):
        self.__try_connect()
        return self

    def __del__(self):
        """ Clean up the socket when this class gets garbage collected. """
        self.close()

    def __exit__(self):
        """ Clean up the socket when this class gets garbage collected. """
        self.close()

    def close(self):
        if self.__active_connection:
            self.sftp.close()
            self.transport.close()
            self.__active_connection = False

    def match_files_for_table(self, files, table_name, search_pattern):
        LOGGER.info("Searching for files for table '%s', matching pattern: %s", table_name, table_pattern)
        matcher = re.compile(search_pattern)
        return [f for f in files if matcher.search(f["filepath"])]

    def get_files_by_prefix(self, prefix):
        """
        Accesses the underlying file system and gets all files that match "prefix", in this case, a directory path.

        Returns a list of filepaths from the root.
        """
        files = []

        if prefix is None or prefix == '':
            prefix = '.'

        try:
            result = self.sftp.listdir_attr(prefix)
        except FileNotFoundError as e:
            raise Exception("Directory '{}' does not exist".format(prefix)) from e

        is_empty = lambda a: a.st_size == 0
        is_directory = lambda a: stat.S_ISDIR(a.st_mode)
        for file_attr in result:
            # NB: This only looks at the immediate level beneath the prefix directory
            if is_directory(file_attr):
                files += self.get_files_by_prefix(prefix + '/' + file_attr.filename)
            else:
                if is_empty(file_attr):
                    continue
                # NB: SFTP specifies path characters to be '/'
                #     https://tools.ietf.org/html/draft-ietf-secsh-filexfer-13#section-6
                files.append({"filepath": prefix + '/' + file_attr.filename,
                              "last_modified": datetime.utcfromtimestamp(file_attr.st_mtime).replace(tzinfo=pytz.UTC)})

        return files

    def get_files(self, prefix, search_pattern, modified_since=None):
        files = self.get_files_by_prefix(prefix)
        if files:
            LOGGER.info('Found %s files in "%s"', len(files), prefix)
        else:
            LOGGER.warning('Found no files on specified SFTP server at "%s"', prefix)

        matching_files = self.get_files_matching_pattern(files, search_pattern)

        if matching_files:
            LOGGER.info('Found %s files in "%s" matching "%s"', len(matching_files), prefix, search_pattern)
        else:
            LOGGER.warning('Found no files on specified SFTP server at "%s" matching "%s"', prefix, search_pattern)

        for f in matching_files:
            LOGGER.info("Found file: %s", f['filepath'])

        if modified_since is not None:
            matching_files = [f for f in matching_files if f["last_modified"] > modified_since]

        return matching_files

    def get_file_handle(self, f):
        """ Takes a file dict {"filepath": "...", "last_modified": "..."} and returns a handle to the file. """
        return self.sftp.open(f["filepath"], 'rb')

    def get_files_matching_pattern(self, files, pattern):
        """ Takes a file dict {"filepath": "...", "last_modified": "..."} and a regex pattern string, and returns files matching that pattern. """
        matcher = re.compile(pattern)
        return [f for f in files if matcher.search(f["filepath"])]

def connection(config):
    return SFTPConnection(config['host'],
                          config['username'],
                          password=config.get('password'),
                          private_key_file=config.get('private_key_file'),
                          port=config.get('port'))
