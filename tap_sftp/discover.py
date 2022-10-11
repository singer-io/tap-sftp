import gzip
import itertools
import json
import socket
import zipfile
import backoff
import singer
import csv as python_csv
from singer_encodings import json_schema, csv, compression, jsonl, schema as se_schema
from singer import metadata
from tap_sftp import client

LOGGER = singer.get_logger()

def compression_infer_local(iterable, file_name):
    """Uses the incoming file_name and checks the end of the string for supported compression types"""
    if not file_name:
        raise Exception("Need file name")

    if file_name.endswith('.tar.gz'):
        raise NotImplementedError("tar.gz not supported")
    elif file_name.endswith('.gz'):
        yield gzip.GzipFile(fileobj=iterable)
    elif file_name.endswith('.zip'):
        with zipfile.ZipFile(iterable) as zip:
            for name in zip.namelist():
                yield zip.open(name)
    else:
        yield iterable

def get_row_iterators_local(iterable, options={}, infer_compression=False, headers_in_catalog=None, with_duplicate_headers=False):
    """
    Accepts an iterable, options, compression flag, catalog headers and flag for duplicate headers
    to infer compression and yields csv.DictReader objects can be used to yield CSV rows.
    """
    if infer_compression:
        compressed_iterables = compression_infer_local(iterable, options.get('file_name'))

    for item in compressed_iterables:

        # Try to parse as JSONL
        try:
            # Duplicate 'item' iterator as 'get_JSONL_iterators' will use 1st row of 'item' to load as JSONL
            # Thus, the 'item' will have records after the 1st row on encountering the 'JSONDecodeError' error
            # As a result 'csv.get_row_iterator' will sync records after the 1st row
            item, item_for_JSONL = itertools.tee(item)
            yield ('jsonl', jsonl.get_JSONL_iterators(item_for_JSONL, options))
            continue
        except json.JSONDecodeError:
            pass

        # Maximize the CSV field width
        csv.maximize_csv_field_width()

        # Finally parse as CSV
        yield ('csv', csv.get_row_iterator(item, options=options))


# pylint: disable=too-many-arguments
def sample_files_local(conn, table_spec, files, sample_rate=1, max_records=1000, max_files=5):
    """Function to sample matched files as per the sampling rate and the max records to sample"""
    LOGGER.info("Sampling files (max files: %s)", max_files)
    to_return = []
    empty_samples = []

    files_so_far = 0

    sorted_files = sorted(
        files, key=lambda f: f['last_modified'], reverse=True)

    for f in sorted_files:
        empty_file, samples = sample_file_local(conn, table_spec, f, sample_rate, max_records)

        if empty_file:
            empty_samples += samples
        else:
            to_return += samples

        files_so_far += 1

        if files_so_far >= max_files:
            break

    if len(to_return) == 0:
        return empty_samples

    return to_return


def sample_file_local(conn, table_spec, f, sample_rate, max_records):
    """Function to sample a file and return list of records for that file"""

    LOGGER.info('Sampling %s (max records: %s, sample rate: %s)',
                f['filepath'],
                max_records,
                sample_rate)

    samples = []
    file_name = f['filepath']

    try:
        file_handle = conn.get_file_handle(f)
    except OSError:
        return (False, samples)

    # Add file_name to opts and flag infer_compression to support gzipped files
    opts = {'key_properties': table_spec['key_properties'],
            'delimiter': table_spec.get('delimiter', ','),
            'file_name': file_name}

    readers = csv.get_row_iterators(file_handle, options=opts, infer_compression=True)

    for _, reader in readers:
        current_row = 0
        for row in reader:
            if (current_row % sample_rate) == 0:
                if row.get(csv.SDC_EXTRA_COLUMN):
                    row.pop(csv.SDC_EXTRA_COLUMN)
                samples.append(row)

            current_row += 1

            if len(samples) >= max_records:
                break

    LOGGER.info("Sampled %s rows from %s", len(samples), file_name)
    # Empty sample to show field selection, if needed
    empty_file = False
    if len(samples) == 0:
        empty_file = True
        # If the 'reader' is an instance of 'csv.Dictreader' and has
        # fieldnames, prepare samples with 'None' field names
        # Assumes all reader objects in readers have the same fieldnames
        if isinstance(reader, python_csv.DictReader) and reader.fieldnames is not None:
            samples.append({name: None for name in reader.fieldnames})

    return (empty_file, samples)

def get_schema_for_table_local(conn, table_spec, sample_rate=1):
    """Function to generate schema for the provided data files"""
    files = conn.get_files(table_spec)

    if not files:
        return {}

    samples = json_schema.sample_files(conn, table_spec, files, sample_rate=sample_rate)

    # Return empty if there is no schema generated
    if not any(samples):
        return {
            'type': 'object',
            'properties': {},
        }

    schema = se_schema.generate_schema(samples, table_spec)

    data_schema = {
        **schema,
        **json_schema.get_sdc_columns()
    }

    return {
        'type': 'object',
        'properties': data_schema,
    }


# Override singer_encoding's 'get_row_iterators' as:
# - The Tap is supporting files created without an extension
csv.get_row_iterators = get_row_iterators_local

# Override singer_encoding's 'sample_file' as:
# - The Tap is looping over the files in the sorted manner of 'last_modified'
# - The Tap is not supporting the skipping of CSV and JSONL files with the wrong extension
json_schema.sample_files = sample_files_local

# Override singer_encoding's 'sample_file' as:
# - The Tap is not having support for CSV files with duplicate headers
# - The Tap is creating a sample record with 'None' for CSV files with only headers
json_schema.sample_file = sample_file_local

def discover_streams(config):
    streams = []

    conn = client.connection(config)

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
    schema = get_schema_for_table_local(conn, table_spec)
    stream_md = metadata.get_standard_metadata(schema,
                                               key_properties=table_spec.get('key_properties'),
                                               replication_method='INCREMENTAL')

    return schema, stream_md
