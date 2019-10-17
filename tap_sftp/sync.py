import json
import csv
import singer
from singer import metadata, utils, Transformer
from tap_sftp import client
from tap_sftp import sampling

LOGGER = singer.get_logger()

def sync_stream(config, state, stream):
    table_name = stream.tap_stream_id
    modified_since = utils.strptime_to_utc(singer.get_bookmark(state, table_name, 'modified_since') or
                                           config['start_date'])

    LOGGER.info('Syncing table "%s".', table_name)
    LOGGER.info('Getting files modified since %s.', modified_since)

    conn = client.connection(config)
    table_config = [c for c in json.loads(config["tables"]) if c["table_name"]==table_name]
    if len(table_config) == 0:
        LOGGER.info("No table configuration found for '%s', skipping stream", table_name)
        return 0
    if len(table_config) > 1:
        LOGGER.info("Multiple table configurations found for '%s', skipping stream", table_name)
        return 0
    table_config = table_config[0]

    files = conn.get_files(table_config["search_prefix"],
                           table_config["search_pattern"],
                           modified_since)

    LOGGER.info('Found %s files to be synced.', len(files))

    records_streamed = 0
    if not files:
        return records_streamed

    for f in files:
        records_streamed += sync_file(conn, f, stream, table_config.get('delimiter', ','))
        state = singer.write_bookmark(state, table_name, 'modified_since', f['last_modified'].isoformat())
        singer.write_state(state)

    LOGGER.info('Wrote %s records for table "%s".', records_streamed, table_name)

    return records_streamed

def sync_file(conn, f, stream, delimiter=','):
    LOGGER.info('Syncing file "%s".', f["filepath"])

    file_handle = conn.get_file_handle(f)
    # TODO Make sure this replace thing is a generator
    reader = csv.DictReader((line.replace('\0', '') for line in file_handle),
                            restkey=sampling.SDC_EXTRA_COLUMN,
                            delimiter=delimiter)

    records_synced = 0

    with Transformer() as transformer:
        for row in reader:
            custom_columns = {
                '_sdc_source_file': f["filepath"],

                # index zero, +1 for header row
                '_sdc_source_lineno': records_synced + 2
            }
            rec = {**row, **custom_columns}

            to_write = transformer.transform(rec, stream.schema.to_dict(), metadata.to_map(stream.metadata))

            singer.write_record(stream.tap_stream_id, to_write)
            records_synced += 1

    return records_synced
