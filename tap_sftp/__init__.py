import json
import sys
import singer

from singer import metadata
from singer import utils
from singer_encodings.utils import is_valid_encoding
from tap_sftp.discover import discover_streams
from tap_sftp.sync import sync_stream
from tap_sftp.stats import STATS

REQUIRED_CONFIG_KEYS = ["username", "port", "private_key_file", "host"]
LOGGER = singer.get_logger()
DEFAULT_ENCODING_FORMAT = "utf-8"

def do_discover(config):
    LOGGER.info("Starting discover")
    # validate the encoding format
    encoding_format = config.get("encoding_format") or DEFAULT_ENCODING_FORMAT
    if not is_valid_encoding(encoding_format):
        raise Exception("Unknown Encoding - {}. Enter the valid encoding format".format(encoding_format))
    streams = discover_streams(config, encoding_format)
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
        # sort STATS data in order of "last_modified" for summary as python3.5 re-arranges the data
        sorted_data = sorted(table_data["files"].items(), key=lambda a: a[1]["last_modified"])
        for filepath, file_data in sorted_data:
            rows.append([table_name,
                         table_data['search_prefix'],
                         table_data['search_pattern'],
                         filepath,
                         file_data['row_count'],
                         file_data['last_modified']])

    LOGGER.info("\n**** Sync Summary:")
    LOGGER.info(next(iter(headers), None))
    for row in rows:
        LOGGER.info(row)
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
