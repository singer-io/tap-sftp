import json
import sys
import singer

from singer import metadata
from singer import utils
from tap_sftp.discover import discover_streams
from tap_sftp.sync import sync_stream
from tap_sftp.stats import STATS
from terminaltables import AsciiTable

REQUIRED_CONFIG_KEYS = ["username", "port", "private_key_file", "host"]
LOGGER = singer.get_logger()

def do_discover(config):
    LOGGER.info("Starting discover")
    streams = discover_streams(config)
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")

def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)

def do_sync(config, catalog, state):
    LOGGER.info('Starting sync.')

    for stream in catalog.streams:
        stream_name = stream.tap_stream_id
        mdata = metadata.to_map(stream.metadata)

        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        singer.write_state(state)
        key_properties = metadata.get(metadata.to_map(stream.metadata), (), "table-key-properties")
        singer.write_schema(stream_name, stream.schema.to_dict(), key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(config, state, stream)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    headers = [['table_name',
                'search prefix',
                'search pattern',
                'file path',
                'row count',
                'last_modified']]

    rows = []

    for table_name, table_data in STATS.items():
        for filepath, file_data in table_data['files'].items():
            rows.append([table_name,
                         table_data['search_prefix'],
                         table_data['search_pattern'],
                         filepath,
                         file_data['row_count'],
                         file_data['last_modified']])

    data = headers + rows
    table = AsciiTable(data, title='Extraction Summary')
    LOGGER.info("\n\n%s", table.table)


    LOGGER.info('Done syncing.')

@singer.utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover(args.config)
    elif args.catalog or args.properties:
        do_sync(args.config, args.catalog, args.state)

if __name__ == '__main__':
    main()
