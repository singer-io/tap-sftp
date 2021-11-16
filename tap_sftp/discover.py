import json
import socket
import backoff
import singer

from singer_encodings import json_schema
from singer import metadata
from tap_sftp import client

LOGGER= singer.get_logger()

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

@backoff.on_exception(backoff.expo,
                      (socket.timeout),
                      max_tries=5,
                      factor=2)
# generate schema
def get_schema(conn, table_spec):
    LOGGER.info('Sampling records to determine table JSON schema "%s".', table_spec['table_name'])
    schema = json_schema.get_schema_for_table(conn, table_spec)
    stream_md = metadata.get_standard_metadata(schema,
                                               key_properties=table_spec.get('key_properties'),
                                               replication_method='INCREMENTAL')

    return schema, stream_md
