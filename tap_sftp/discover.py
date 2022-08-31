import io
import itertools
import json
import socket
import sys
import backoff
import singer
import gzip
import zipfile
import csv as python_csv
from singer_encodings import json_schema, csv
from singer import metadata
from tap_sftp import client, gzip_utils, schema

LOGGER= singer.get_logger()

def compression_infer_local(iterable, file_name):
    """Uses the incoming file_name and checks the end of the string for supported compression types"""
    if not file_name:
        raise Exception("Need file name")

    if file_name.endswith('.tar.gz'):
        raise NotImplementedError("tar.gz not supported")
    elif file_name.endswith('.gz'):
        file_bytes = iterable.read()
        gz_file_name = None
        try:
            gz_file_name = gzip_utils.get_file_name_from_gzfile(fileobj=io.BytesIO(file_bytes))
        except AttributeError:
            # If a file is compressed using gzip command with --no-name attribute,
            # It will not return the file name and timestamp. Hence we will skip such files.
            LOGGER.warning('Skipping "%s" file as we did not get the original file name.', file_name)
        # Send file object and file name
        yield [gzip.GzipFile(fileobj=io.BytesIO(file_bytes)), gz_file_name]
    elif file_name.endswith('.zip'):
        with zipfile.ZipFile(iterable) as zip:
            for name in zip.namelist():
                yield zip.open(name)
    else:
        yield iterable

def maximize_csv_field_width():
    """Set the max filed size as per the system's maxsize"""

    current_field_size_limit = csv.csv.field_size_limit()
    field_size_limit = sys.maxsize

    if current_field_size_limit != field_size_limit:
        csv.csv.field_size_limit(field_size_limit)
        LOGGER.info("Changed the CSV field size limit from %s to %s",
                    current_field_size_limit,
                    field_size_limit)

def sample_file_local(conn, table_spec, f, sample_rate, max_records):
    samples = []
    try:
        file_handle = conn.get_file_handle(f)
    except OSError:
        return (False, samples)

    # Add file_name to opts and flag infer_compression to support gzipped files
    opts = {'key_properties': table_spec['key_properties'],
            'delimiter': table_spec['delimiter'],
            'file_name': f['filepath']}

    readers = get_row_iterators_local(file_handle, options=opts, infer_compression=True)

    for reader in readers:
        current_row = 0
        for row in reader:
            if (current_row % sample_rate) == 0:
                if row.get(csv.SDC_EXTRA_COLUMN):
                    row.pop(csv.SDC_EXTRA_COLUMN)
                samples.append(row)

            current_row += 1

            if len(samples) >= max_records:
                break

    # Empty sample to show field selection, if needed
    empty_file = False
    if len(samples) == 0:
        empty_file = True
        # If the 'reader' is an instance of 'csv.Dictreader' and has
        # fieldnames, prepare samples with None field names
        # Assumes all reader objects in readers have the same fieldnames
        if isinstance(reader, python_csv.DictReader) and reader.fieldnames is not None:
            samples.append({name: None for name in reader.fieldnames})

    return (empty_file, samples)

def get_row_iterators_local(iterable, options={}, infer_compression=False):
    """Accepts an interable, options and a flag to infer compression and yields
    csv.DictReader objects which can be used to yield CSV rows."""
    if infer_compression:
        compressed_iterables = compression_infer_local(iterable, options.get('file_name'))

    for item in compressed_iterables:
        file_name_splitted = options.get('file_name').split('.')
        extension = file_name_splitted[-1].lower()
        # Get the extension of the zipped file
        if extension == 'zip':
            extension = item.name.split('.')[-1].lower()
        # Get the extension of the gzipped file ie. file.csv.gz -> csv
        elif extension == 'gz':
            # Get file name
            gzip_file_name = item[1]
            # Set iterator 'item'
            item = item[0]
            # Get file extension
            extension = gzip_file_name.split('.')[-1].lower() if gzip_file_name else gzip_file_name

        # For GZ files, if the file is gzipped with --no-name, then
        # the 'extension' will be 'None'. Hence, send an empty list
        if not extension:
            yield []
        # If the extension is JSONL then use 'get_JSONL_iterators'
        elif extension == 'jsonl':
            yield get_JSONL_iterators(item, options)
        # Assuming the extension is 'csv' of 'txt', then use singer_encoding's 'get_row_iterator'
        else:
            # Maximize the CSV field width
            maximize_csv_field_width()
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
            raise Exception('JSONL file missing required headers: {}'
                            .format(key_properties - all_keys))

    if options.get('date_overrides'):
        date_overrides = set(options['date_overrides'])
        if not date_overrides.issubset(all_keys):
            raise Exception('JSONL file missing date_overrides headers: {}'
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

# Override singer_encoding's 'sample_file' as per the Tap's JSONL support
json_schema.sample_file = sample_file_local

# Override singer_encoding's 'generate_schema' as the Tap's JSONL support
json_schema.generate_schema = schema.generate_schema

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
