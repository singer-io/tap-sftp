import re

from . import csv

SDC_SOURCE_FILE_COLUMN = "_sdc_source_file"
SDC_SOURCE_LINENO_COLUMN = "_sdc_source_lineno"

# TODO: Add additional logging

# TODO: conn needs get_files and get_file_handle functions
def get_schema_for_table(conn, table_spec, encoding_format = "utf-8"):
    files = conn.get_files(table_spec['search_prefix'], table_spec['search_pattern'])

    if not files:
        return {}

    samples = sample_files(conn, table_spec, files, encoding_format=encoding_format)

    schema = generate_schema(samples, table_spec)

    # return empty if there is no schema generated
    if not schema:
        return {}

    data_schema = {
        **schema,
        SDC_SOURCE_FILE_COLUMN: {'type': 'string'},
        SDC_SOURCE_LINENO_COLUMN: {'type': 'integer'},
        csv.SDC_EXTRA_COLUMN: {'type': 'array', 'items': {'type': 'string'}},
    }

    return {
        'type': 'object',
        'properties': data_schema,
    }

def sample_file(conn, table_spec, f, sample_rate, max_records, encoding_format = "utf-8"):
    table_name = table_spec['table_name']
    plurality = "s" if sample_rate != 1 else ""

    samples = []
    try:
        file_handle = conn.get_file_handle(f)
    except OSError:
        return (False, samples)

    # Add file_name to opts and flag infer_compression to support gzipped files
    opts = {'key_properties': table_spec['key_properties'],
            'delimiter': table_spec['delimiter'],
            'file_name': f['filepath']}

    readers = csv.get_row_iterators(file_handle, options=opts, infer_compression=True, encoding_format=encoding_format)

    for reader in readers:
        current_row = 0
        for row in reader:
            if (current_row % sample_rate) == 0:
                if row.get(csv.SDC_EXTRA_COLUMN):
                    row.pop(csv.SDC_EXTRA_COLUMN)
                samples.append(row)

            current_row += 1

            if len(samples) >= max_records:
                break

    # Empty sample to show field selection, if needed
    empty_file = False
    if len(samples) == 0:
        empty_file = True
        # Assumes all reader objects in readers have the same fieldnames
        if reader.fieldnames is not None:
            samples.append({name: None for name in reader.fieldnames})

    return (empty_file, samples)

# pylint: disable=too-many-arguments
def sample_files(conn, table_spec, files,
                 sample_rate=1, max_records=1000, max_files=5, encoding_format = "utf-8"):
    to_return = []
    empty_samples = []

    files_so_far = 0

    sorted_files = sorted(files, key=lambda f: f['last_modified'], reverse=True)

    for f in sorted_files:
        empty_file, samples = sample_file(conn, table_spec, f,
                                          sample_rate, max_records, encoding_format)

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

def infer(datum):
    """
    Returns the inferred data type
    """
    if datum is None or datum == '':
        return None

    try:
        int(datum)
        return 'integer'
    except (ValueError, TypeError):
        pass

    try:
        #numbers are NOT floats, they are DECIMALS
        float(datum)
        return 'number'
    except (ValueError, TypeError):
        pass

    return 'string'

def count_sample(sample, counts, table_spec):
    for key, value in sample.items():
        if key not in counts:
            counts[key] = {}

        date_overrides = table_spec.get('date_overrides', [])
        if key in date_overrides:
            datatype = "date-time"
        else:
            datatype = infer(value)

        if datatype is not None:
            counts[key][datatype] = counts[key].get(datatype, 0) + 1

    return counts

def pick_datatype(counts):
    """
    If the underlying records are ONLY of type `integer`, `number`,
    or `date-time`, then return that datatype.

    If the underlying records are of type `integer` and `number` only,
    return `number`.

    Otherwise return `string`.
    """
    to_return = 'string'

    if counts.get('date-time', 0) > 0:
        return 'date-time'

    if len(counts) == 1:
        if counts.get('integer', 0) > 0:
            to_return = 'integer'
        elif counts.get('number', 0) > 0:
            to_return = 'number'

    elif(len(counts) == 2 and
         counts.get('integer', 0) > 0 and
         counts.get('number', 0) > 0):
        to_return = 'number'

    return to_return

def generate_schema(samples, table_spec):
    counts = {}
    for sample in samples:
        # {'name' : { 'string' : 45}}
        counts = count_sample(sample, counts, table_spec)

    for key, value in counts.items():
        datatype = pick_datatype(value)

        if datatype == 'date-time':
            counts[key] = {
                'anyOf': [
                    {'type': ['null', 'string'], 'format': 'date-time'},
                    {'type': ['null', 'string']}
                ]
            }
        else:
            types = ['null', datatype]
            if datatype != 'string':
                types.append('string')
            counts[key] = {
                'type': types,
            }

    return counts
