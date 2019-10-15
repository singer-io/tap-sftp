from singer import metadata
from tap_sftp import client
from tap_sftp import sampling

def discover_streams(config):
    streams = []

    conn = client.connection(config)
    prefix = format(config.get("user_dir", "./"))

    for table_spec in config['tables']:
        schema = sampling.get_sampled_schema_for_table(conn, config["path"], exported_table)
        streams.append(
            {
                'stream': table_spec['table_name'],
                'tap_stream_id': table_spec['table_name'],
                'schema': schema,
                'metadata': load_metadata(table_spec, schema)
            }
        )

    return streams


def load_metadata(schema):
    mdata = metadata.new()

    key_properties = [sampling.SDC_SOURCE_FILE_COLUMN, sampling.SDC_SOURCE_LINENO_COLUMN]
    mdata = metadata.write(mdata, (), 'table-key-properties', key_properties)

    # Make all fields automatic
    for field_name in schema.get('properties', {}).keys():
        mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')

    return metadata.to_list(mdata)
