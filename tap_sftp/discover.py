import io
import itertools
import json
import socket
import backoff
import singer
import gzip
import zipfile

from singer_encodings import json_schema, csv, compression
from singer import metadata
from tap_sftp import client, gzip_utils

LOGGER= singer.get_logger()

def compression_infer_local(iterable, file_name):
    """Uses the incoming file_name and checks the end of the string for supported compression types"""
    if not file_name:
        raise Exception("Need file name")

    if file_name.endswith('.tar.gz'):
        raise NotImplementedError("tar.gz not supported")
    elif file_name.endswith('.gz'):
        file_bytes = iterable.read()
        # Send file object and file name
        yield [gzip.GzipFile(fileobj=io.BytesIO(file_bytes)), \
            gzip_utils.get_file_name_from_gzfile(fileobj=io.BytesIO(file_bytes))]
    elif file_name.endswith('.zip'):
        with zipfile.ZipFile(iterable) as zip:
            for name in zip.namelist():
                yield zip.open(name)
    else:
        yield iterable

def get_row_iterators_local(iterable, options={}, infer_compression=False):
    """Accepts an interable, options and a flag to infer compression and yields
    csv.DictReader objects which can be used to yield CSV rows."""
    if infer_compression:
        compressed_iterables = compression.infer(iterable, options.get('file_name'))

    for item in compressed_iterables:
        file_name_splitted = options.get('file_name').split('.')
        extension = file_name_splitted[-1]
        # Get the extension of the zipped file
        if extension == 'zip':
            extension = item.name.split('.')[-1]
        # Get the extension of the gzipped file ie. file.csv.gz -> csv
        if extension == 'gz':
            # Get file name
            gzip_file_name = item[1]
            # Set iterator 'item'
            item = item[0]
            # Get file extension
            extension = gzip_file_name.split('.')[-1]

        # If the extension is JSONL then use 'get_JSONL_iterators'
        if extension == 'jsonl':
            yield get_JSONL_iterators(item, options)
        # Assuming the extension is 'csv' of 'txt', then use singer_encoding's 'get_row_iterator'
        else:
            yield csv.get_row_iterator(item, options=options)

def get_JSONL_iterators(iterator, options):
    # Get JSOL rows
    records = get_JSONL_rows(iterator)
    check_jsonl_sample_records, records = itertools.tee(records)

    # Veirfy the 'date_overrides' and 'key_properties' as per the config
    check_key_properties_and_date_overrides_for_jsonl_file(options, check_jsonl_sample_records)
    return records

def check_key_properties_and_date_overrides_for_jsonl_file(options, jsonl_sample_records):

    all_keys = set()
    for record in jsonl_sample_records:
        keys = record.keys()
        all_keys.update(keys)

    if options.get('key_properties'):
        key_properties = set(options['key_properties'])
        if not key_properties.issubset(all_keys):
            raise Exception('CSV file missing required headers: {}'
                            .format(key_properties - all_keys))

    if options.get('date_overrides'):
        date_overrides = set(options['date_overrides'])
        if not date_overrides.issubset(all_keys):
            raise Exception('CSV file missing date_overrides headers: {}'
                            .format(date_overrides - all_keys))

def get_JSONL_rows(iterator):
    # Return JSON rows from JSONL file
    for row in iterator:
        decoded_row = row.decode('utf-8')
        if decoded_row.strip():
            row = json.loads(decoded_row)
        else:
            continue

        yield row

# Override singer_encoding's 'get_row_iterators' as per the Tap's JSONL support
csv.get_row_iterators = get_row_iterators_local

# Override singer_encoding's 'infer' as per the Tap's JSONL support for GZIP files
compression.infer = compression_infer_local

# Override the '_sdc_extra' column value as per the JSONL-supported format
json_schema.SDC_EXTRA_VALUE = {
    'type': 'array',
    'items': {
        'anyOf': [
            {'type': 'object', 'properties': {}},
            {'type': 'string'}
        ]
    }
}

def discover_streams(config):
    streams = []

    conn = client.connection(config)
    prefix = format(config.get("user_dir", "./"))

    tables = json.loads(config['tables'])
    for table_spec in tables:
        schema, stream_md = get_schema(conn, table_spec)

        streams.append(
            {
                'stream': table_spec['table_name'],
                'tap_stream_id': table_spec['table_name'],
                'schema': schema,
                'metadata': stream_md
            }
        )

    return streams

# backoff for 60 seconds as the request will again backoff again
# in 'client.get_files_by_prefix' when 'Timeout' error occurs
@backoff.on_exception(backoff.constant,
                      (socket.timeout),
                      max_time=60,
                      interval=10,
                      jitter=None)
# generate schema
def get_schema(conn, table_spec):
    LOGGER.info('Sampling records to determine table JSON schema "%s".', table_spec['table_name'])
    schema = json_schema.get_schema_for_table(conn, table_spec)
    stream_md = metadata.get_standard_metadata(schema,
                                               key_properties=table_spec.get('key_properties'),
                                               replication_method='INCREMENTAL')

    return schema, stream_md
