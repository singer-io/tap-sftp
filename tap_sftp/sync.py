import json
import socket
import backoff
import singer
from singer import metadata, utils, Transformer
from tap_sftp import client
from tap_sftp import stats
from singer_encodings import csv

LOGGER = singer.get_logger()

def sync_stream(config, state, stream):
    table_name = stream.tap_stream_id
    modified_since = utils.strptime_to_utc(singer.get_bookmark(state, table_name, 'modified_since') or
                                           config['start_date'])

    LOGGER.info('Syncing table "%s".', table_name)
    LOGGER.info('Getting files modified since %s.', modified_since)

    conn = client.connection(config)
    table_spec = [c for c in json.loads(config["tables"]) if c["table_name"]==table_name]
    if len(table_spec) == 0:
        LOGGER.info("No table configuration found for '%s', skipping stream", table_name)
        return 0
    if len(table_spec) > 1:
        LOGGER.info("Multiple table configurations found for '%s', skipping stream", table_name)
        return 0
    table_spec = table_spec[0]

    files = conn.get_files(table_spec, modified_since)

    LOGGER.info('Found %s files to be synced.', len(files))

    records_streamed = 0
    if not files:
        return records_streamed

    for f in files:
        records_streamed += sync_file(conn, f, stream, table_spec)
        state = singer.write_bookmark(state, table_name, 'modified_since', f['last_modified'].isoformat())
        singer.write_state(state)

    LOGGER.info('Wrote %s records for table "%s".', records_streamed, table_name)

    return records_streamed

# retry 5 times for timeout error
@backoff.on_exception(backoff.expo,
                      (socket.timeout),
                      max_tries=5,
                      factor=2)
def sync_file(conn, f, stream, table_spec):
    LOGGER.info('Syncing file "%s".', f["filepath"])

    try:
        file_handle = conn.get_file_handle(f)
    except OSError:
        return 0

    # Add file_name to opts and flag infer_compression to support gzipped files
    opts = {'key_properties': table_spec['key_properties'],
            'delimiter': table_spec['delimiter'],
            'file_name': f['filepath']}

    readers = csv.get_row_iterators(file_handle, options=opts, infer_compression=True)

    records_synced = 0
    tap_added_fields =  ['_sdc_source_file', '_sdc_source_lineno', 'sdc_extra']
    schema_dict = stream.schema.to_dict()

    for file_extension, reader in readers:
        with Transformer() as transformer:
            # Row start for files as per the file type
            row_start_line = 1 if file_extension == 'jsonl' else 2
            for row in reader:
                # Skipping the empty line
                if len(row) == 0:
                    continue

                custom_columns = {
                    '_sdc_source_file': f["filepath"],
                    '_sdc_source_lineno': records_synced + row_start_line
                }

                # For CSV files, the '_sdc_extra' is handled by 'restkey' in 'csv.DictReader'
                # If the file is JSONL then prepare '_sdc_extra' column
                if file_extension == 'jsonl':
                    sdc_extra = []

                    # Get the extra fields ie. (json keys - fields from the catalog - fields added by the tap)
                    extra_fields = set()
                    # Create '_sdc_extra' fields if the schema is not empty
                    if schema_dict.get('properties'):
                        extra_fields = set(row.keys()) - set(schema_dict.get('properties', {}).keys() - tap_added_fields)

                    # Prepare list of extra fields
                    for extra_field in extra_fields:
                        sdc_extra.append({extra_field: row.get(extra_field)})
                    # If the record contains extra fields, then add the '_sdc_extra' column
                    if extra_fields:
                        custom_columns['_sdc_extra'] = sdc_extra

                rec = {**row, **custom_columns}

                to_write = transformer.transform(rec, schema_dict, metadata.to_map(stream.metadata))

                singer.write_record(stream.tap_stream_id, to_write)
                records_synced += 1

    stats.add_file_data(table_spec, f['filepath'], f['last_modified'], records_synced)

    return records_synced
