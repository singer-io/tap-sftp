import csv
import singer

SDC_EXTRA_COLUMN = "_sdc_extra"
NO_HEADERS = "no_headers"

LOGGER = singer.get_logger()

class CSVHelper:
    """
        CSV Helper class to read the CSV with duplicate headers too.
    """

    def __init__(self):
        self.all_csv_headers = []
        self.unique_headers = []
        self.unique_headers_idxs = []
        self.duplicate_headers = []
        self.dup_headers_idxs = []


    @staticmethod
    def generate_dict_from_zipped_data(zipped_data):
        """
        Generate dictionary from zipped data.
        If same key exist multiple time it will store the value in list.
            e.g.: 1. {'key': 'value'}
                  2. {'key': ['value1', 'value2']}

        Args:
            zipped_data zip(): Zipped Data
                e.g. ('key','value')

        Returns:
            dict() : Dictionary of Zipped Data
        """
        dup_dictionary = {}

        for key, value in zipped_data:
            if key in dup_dictionary:
                # Check the type of value of dictionary whether it list
                if not isinstance(dup_dictionary[key], list):
                    dup_dictionary[key] = [dup_dictionary[key], value]
                else:
                    dup_dictionary[key].append(value)
            else:
                dup_dictionary[key] = value

        return dup_dictionary


    def __generate_dict_reader(self, csv_reader):
        """
        Generate dictionary from CSV reader object

        Args:
            csv_reader csv.Reader() : Reader object of CSV
        Return:
            yield row
        """

        for row in csv_reader:
            row_dict = {}
            row_length = len(row)
            all_csv_headers_length = len(self.all_csv_headers)
            sdc_extra_values = []

            if len(self.dup_headers_idxs) > 0:
                new_unique_headers_idxs = self.unique_headers_idxs
                new_dup_headers_idxs = self.dup_headers_idxs

                # If number of values provided in the row are lesser than or equal to CSV headers count
                if row_length <= all_csv_headers_length:
                    new_unique_headers_idxs = [index for index in self.unique_headers_idxs if index < row_length]
                    new_dup_headers_idxs = [index for index in self.dup_headers_idxs if index < row_length]

                # If number of values provided in the row are greater than CSV headers
                else:
                    sdc_extra_values.append({ NO_HEADERS : row[all_csv_headers_length:] })

                # Fetching the values of only unique headers
                row_dict = dict(zip(self.unique_headers, map(row.__getitem__, new_unique_headers_idxs)))

                # Fetching the values of duplicate headers
                dup_headers_values = list(map(row.__getitem__, new_dup_headers_idxs))

                # Generate zip for duplicate headers and its values
                dup_zipped = zip(self.duplicate_headers, dup_headers_values)

                # Get the dictionary from zipped data
                dup_dictionary = self.generate_dict_from_zipped_data(dup_zipped)

                # Adding dictionary of duplicate header,values in _sdc_extra field as string
                sdc_extra_values.extend([{dup_head : dup_val} for dup_head, dup_val in dup_dictionary.items()])

                if len(sdc_extra_values) > 0:
                    row_dict.update({ SDC_EXTRA_COLUMN : sdc_extra_values})

            else:

                # If row contains more values than number of headers
                if row_length > all_csv_headers_length:
                    row_dict = dict(zip(self.all_csv_headers, row[0:len(self.unique_headers)]))

                    # Adding extra column values in _sdc_extra key
                    row_dict.update({ SDC_EXTRA_COLUMN : [{ NO_HEADERS : row[len(self.unique_headers):] }] })

                else:
                    row_dict = dict(zip(self.all_csv_headers, row))

            yield row_dict


    def get_row_iterator(self, file_stream, delimiter, headers_in_catalog = None):
        """Accepts a file_stream, delimiter and the headers available in catalog. It returns a csv.Reader object
        which can be used to yield CSV rows."""

        # Replace any NULL bytes in the line given to the Reader
        reader = csv.reader((line.replace('\0', '') for line in file_stream), delimiter=delimiter)
        try:
            self.all_csv_headers = next(reader)
        except StopIteration:
            # Return None if CSV file is empty.
            return None
        header_index = 0

        for header in self.all_csv_headers:
            # Checking if the header is present in the headers_in_catalog
            not_in_catalog = headers_in_catalog and header not in headers_in_catalog
            if header in self.unique_headers or not_in_catalog:

                 # check whether header is not in catalog and it is not already in duplicate headers list
                if not_in_catalog and header not in self.duplicate_headers:
                    LOGGER.warn("\"%s\" field is not found in catalog and its value will be stored in the \"_sdc_extra\" field.",header)

                self.duplicate_headers.append(header)
                self.dup_headers_idxs.append(header_index)
            else:
                self.unique_headers.append(header)
                self.unique_headers_idxs.append(header_index)
            header_index += 1


        # Check whether duplicate headers are present or not
        if len(self.dup_headers_idxs) > 0:
            # Get the unique names of duplicate headers
            dup_headers = set(map(self.all_csv_headers.__getitem__, self.dup_headers_idxs))
            LOGGER.warn("Duplicate Header(s) %s found in the csv and its value will be stored in the \"_sdc_extra\" field.",dup_headers)

        return self.__generate_dict_reader(reader)
