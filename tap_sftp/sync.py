from singer import metadata, utils, Transformer

import singer
import singer_encodings.csv as csv
from tap_sftp import client

LOGGER = singer.get_logger()

def sync_stream(config, state, stream):
    table_name = stream.tap_stream_id
    modified_since = utils.strptime_to_utc(singer.get_bookmark(state, table_name, 'modified_since') or
                                           config['start_date'])

    LOGGER.info('Syncing table "%s".', table_name)
    LOGGER.info('Getting files modified since %s.', modified_since)

    conn = client.connection(config)
    files = conn.get_files_for_table(config["path"], table_name, modified_since)

    LOGGER.info('Found %s files to be synced.', len(files))

    records_streamed = 0
    if not files:
        return records_streamed

    for f in files:
        records_streamed += sync_table_file(conn, f, stream)
        state = singer.write_bookmark(state, table_name, 'modified_since', f['last_modified'].isoformat())
        singer.write_state(state)

    LOGGER.info('Wrote %s records for table "%s".', records_streamed, table_name)

    return records_streamed

def sync_table_file(conn, f, stream):
    LOGGER.info('Syncing file "%s".', f["filepath"])

    table_name = stream.tap_stream_id

    file_handle = conn.get_file_handle(f)
    raw_stream = client.RawStream(file_handle)
    iterator = csv.get_row_iterator(raw_stream)

    records_synced = 0

    for row in iterator:
        custom_columns = {
            '_sdc_source_file': f["filepath"],

            # index zero, +1 for header row
            '_sdc_source_lineno': records_synced + 2
        }
        rec = {**row, **custom_columns}

        with Transformer() as transformer:
            to_write = transformer.transform(rec, stream.schema.to_dict(), metadata.to_map(stream.metadata))

        singer.write_record(table_name, to_write)
        records_synced += 1

    return records_synced
