import unittest
from parameterized import parameterized
from tap_sftp import schema

class SchemaTest(unittest.TestCase):
    '''
        Test class to verify an appropriate working of functions in schema.py file in the tap.
    '''

    def test_generate_schema(self):
        '''
            Test case to verify that for a given sample, the schema is generated correctly.
        '''

        samples = [{'test_key': 'test_value'}, {'TEST': ['test'], 'int_value' : 27}, {'Test': []}]
        table_spec = {'table_name': 'TEST', 'search_prefix': '/test', 'search_pattern': 'jsonl'}

        expected_schema = {
            'test_key': {'type': ['null', 'string']},
            'TEST': {
                'anyOf': [
                    {'items': {'type': ['null', 'string']}, 'type': 'array'},
                     {'type': ['null', 'string']}
                ]
            },
            'int_value': {'type': ['null', 'integer', 'string']},
            'Test': {
                'anyOf': [
                    {'items': {'type': ['null', 'string']}, 'type': 'array'},
                    {'type': ['null', 'string']}
                ]
            }
            }

        actual_schema = schema.generate_schema(samples, table_spec)

        self.assertEqual(expected_schema, actual_schema)

    @parameterized.expand([
        [
            'date_time_datatype',
            "date-time",
            {
                'anyOf': [
                    {'type': ['null', 'string'], 'format': 'date-time'},
                    {'type': ['null', 'string']}
                ]
        }
        ],
        [
            "dictionary_datatype",
            "dict",
            {
                'anyOf': [
                    {'type': 'object', 'properties': {}},
                    {'type': ['null', 'string']}
                ]
        }
        ],
        [
            "string_datatype",
            "string",
            {
                'type': ['null', "string"],
            }
        ],
        [
            "integer_datatype",
            "integer",
            {
                'type': ['null', "integer", "string"],
            }
        ],
        [
            "boolean_datatype",
            "boolean",
            {
                'type': ['null', "boolean", "string"],
            }
        ],
        [
            "float_datatype",
            "number",
            {
                'type': ['null', "number", "string"],
            }
        ]
    ])
    def test_datatype_schema(self, name, test_datatype, expected_schema):
        '''
            Test case to verify that schema is created correctly for individual
            fields depending on the datatype of the field.
        '''

        actual_schema = schema.datatype_schema(test_datatype)

        self.assertEqual(actual_schema, expected_schema)

    @parameterized.expand([
        ['dictionary_datatype', {'dict':62}, 'dict'],
        ['float_datatype', {'number':27}, 'number'],
        ['integer_datatype', {'integer':32}, 'integer'],
        ['float_and_integer_datatype', {'number':18, 'integer':81}, 'number'],
        ['string_datatype', {'string':43}, 'string'],
        ['no_datatype', {}, 'string']
    ])
    def test_pick_datatype(self, name, test_value, expected_datatype):
        '''
            Test case to verify that when a datatype with it's counts is given,
            then the datatype is returned. If no counts are there (which means no datatype),
            then it should return the default value "string".
        '''

        actual_datatype = schema.pick_datatype(test_value)

        self.assertEqual(expected_datatype, actual_datatype)

    def test_count_sample(self):
        '''
            Test case to verify that for a given sample, counts of datatypes are returned
            correctly as per their occurrences.
        '''

        sample = {
            'test_key': {'string_value': '', 'int_value' : 27},
            'float_value' : '45.99',
            'string_value': ''
            }
        table_spec = {'table_name': 'TEST', 'search_prefix': '/test', 'search_pattern': 'jsonl'}
        expected_counts = {'test_key': {'dict':1}, 'float_value' : {'number':1}, 'string_value': {}}

        actual_counts = schema.count_sample(sample, {}, table_spec)

        self.assertEqual(expected_counts, actual_counts)

    @parameterized.expand([
        ['no_datum', 'test', None, None],
        ['empty_list_as_datum', 'test', [], 'list'],
        ['non_empty_list_as_datum', 'test', ['test1'], 'list.string'],
        ['nested_list_as_datum', 'test', [['test2']], 'list.string'],
        ['key_in_date_overrides', 'TEST', 'test1', 'date-time'],
        ['dict_as_datum', 'test', {}, 'dict'],
        ['string_of_integer_as_datum', 'test', '12', 'integer'],
        ['integer_as_datum', 'test', 21, 'integer'],
        ['string_of_float_as_datum', 'test', '12.235', 'number'],
        ['float_as_datum', 'test', 25.487, 'number']
    ])
    def test_infer(self, name, test_key, test_datum, expected_datatype):
        '''
            Test case to verify that proper datatype is returned for corresponding data.
        '''

        actual_datatype = schema.infer(key = test_key, datum = test_datum, date_overrides= ['TEST'])

        self.assertEqual(expected_datatype, actual_datatype)
