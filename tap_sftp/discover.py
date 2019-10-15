from singer import metadata
from tap_sftp import client
from tap_sftp import sampling

def discover_streams(config):
    streams = []

    conn = client.connection(config)
    exported_tables = conn.get_exported_tables(config.get("user_dir", "~/"))

    for exported_table in exported_tables:
        schema = sampling.get_sampled_schema_for_table(conn, config["path"], exported_table)
        streams.append({'stream': exported_table, 'tap_stream_id': exported_table, 'schema': schema, 'metadata': load_metadata(schema)})
    return streams


def load_metadata(schema):
    mdata = metadata.new()

    key_properties = [sampling.SDC_SOURCE_FILE_COLUMN, sampling.SDC_SOURCE_LINENO_COLUMN]
    mdata = metadata.write(mdata, (), 'table-key-properties', key_properties)

    # Make all fields automatic
    for field_name in schema.get('properties', {}).keys():
        mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')

    return metadata.to_list(mdata)
