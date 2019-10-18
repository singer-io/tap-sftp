import singer
from tap_sftp import client, conversion
from singer_encodings import csv

LOGGER = singer.get_logger()

SDC_SOURCE_FILE_COLUMN = "_sdc_source_file"
SDC_SOURCE_LINENO_COLUMN = "_sdc_source_lineno"

def get_sampled_schema_for_table(conn, table_spec):
    LOGGER.info('Sampling records to determine table schema "%s".', table_spec['table_name'])

    files = conn.get_files(table_spec['search_prefix'], table_spec['search_pattern'])

    if not files:
        return {}

    samples = sample_files(conn, table_spec, files)

    metadata_schema = {
        SDC_SOURCE_FILE_COLUMN: {'type': 'string'},
        SDC_SOURCE_LINENO_COLUMN: {'type': 'integer'},
        csv.SDC_EXTRA_COLUMN: {'type': 'array', 'items': {'type': 'string'}},
    }

    data_schema = conversion.generate_schema(samples, table_spec)

    return {
        'type': 'object',
        'properties': merge_dicts(data_schema, metadata_schema)
    }

def sample_file(conn, table_spec, f, sample_rate, max_records):
    table_name = table_spec['table_name']
    plurality = "s" if sample_rate != 1 else ""
    LOGGER.info('Sampling %s (%s records, every %s record%s).', f['filepath'], max_records, sample_rate, plurality)

    samples = []

    file_handle = conn.get_file_handle(f)
    reader = csv.get_row_iterator(file_handle, {'key_properties': table_spec['key_properties']})

    current_row = 0
    for row in reader:
        if (current_row % sample_rate) == 0:
            if row.get(csv.SDC_EXTRA_COLUMN):
                row.pop(csv.SDC_EXTRA_COLUMN)
            samples.append(row)

        current_row += 1

        if len(samples) >= max_records:
            break

    LOGGER.info('Sampled %s records.', len(samples))

    # Empty sample to show field selection, if needed
    empty_file = False
    if len(samples) == 0:
        empty_file = True
        samples.append({name: None for name in iterator.fieldnames})

    return (empty_file, samples)

# pylint: disable=too-many-arguments
def sample_files(conn, table_spec, files,
                 sample_rate=1, max_records=1000, max_files=5):
    to_return = []
    empty_samples = []

    files_so_far = 0

    sorted_files = sorted(files, key=lambda f: f['last_modified'], reverse=True)

    for f in sorted_files:
        empty_file, samples = sample_file(conn, table_spec, f,
                                          sample_rate, max_records)

        if empty_file:
            empty_samples += samples
        else:
            to_return += samples

        files_so_far += 1

        if files_so_far >= max_files:
            break

    if not any(to_return):
        return empty_samples

    return to_return

def merge_dicts(first, second):
    to_return = first.copy()

    for key in second:
        if key in first:
            if isinstance(first[key], dict) and isinstance(second[key], dict):
                to_return[key] = merge_dicts(first[key], second[key])
            else:
                to_return[key] = second[key]

        else:
            to_return[key] = second[key]

    return to_return
