import json
from singer import metadata
from tap_sftp import client
from tap_sftp import sampling

def discover_streams(config):
    streams = []

    conn = client.connection(config)
    prefix = format(config.get("user_dir", "./"))

    tables = json.loads(config['tables'])
    for table_spec in tables:
        schema = sampling.get_sampled_schema_for_table(conn, table_spec)
        stream_md = metadata.get_standard_metadata(schema,
                                                   key_properties=table_spec.get('key_properties'),
                                                   replication_method='INCREMENTAL')
        streams.append(
            {
                'stream': table_spec['table_name'],
                'tap_stream_id': table_spec['table_name'],
                'schema': schema,
                'metadata': stream_md
            }
        )

    return streams
