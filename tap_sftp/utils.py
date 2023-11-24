import codecs
import singer

LOGGER = singer.get_logger()

def is_valid_encoding(encoding_format):
    """
    Check if the provided encoding format is valid.
    Args:
        encoding_format (str): The encoding format to be validated.
    Returns:
        bool: True if the encoding format is valid, False otherwise.
    This function attempts to look up the specified encoding format using Python's `codecs.lookup()`
    method. If a LookupError occurs, indicating an invalid encoding format, a warning is logged, and
    False is returned. Otherwise, True is returned, indicating a valid encoding format.
    """
    try:
        # Attempt to look up the codecs for the specified encoding format
        codecs.lookup(encoding_format)
    except LookupError as err:
        # If a LookupError occurs (invalid encoding format), log a warning
        LOGGER.warning(err)
        return False
    return True
