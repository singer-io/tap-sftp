import simplejson as json
import sys
from singer import RecordMessage


def format_message(message,ensure_ascii=True):
    return json.dumps(message.asdict(), use_decimal=True, ensure_ascii=ensure_ascii)


def write_message(message, ensure_ascii=True):
    sys.stdout.write(format_message(message, ensure_ascii=ensure_ascii) + '\n')
    sys.stdout.flush()


def write_record(stream_name, record, stream_alias=None, time_extracted=None, ensure_ascii=True):
    """
    Write a single record for the given stream.
    
    """
    write_message(RecordMessage(stream=(stream_alias or stream_name),
                                record=record,
                                time_extracted=time_extracted), ensure_ascii=ensure_ascii)
