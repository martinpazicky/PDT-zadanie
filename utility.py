import datetime
import time
from itertools import islice


def replace_null_chars(str):
    return str.replace("\x00", "\uFFFD")


def get_json_field(key, record):
    if key in record:
        return record[key]


def get_nested_json_field(key1, key2, record):
    if key1 in record:
        if key2 in record[key1]:
            return record[key1][key2]


def sec_to_mmss(seconds):
    mins = seconds // 60
    seconds %= 60
    return "%02i:%02i" % (mins, seconds)


def csv_write_info(writer, start, cp):
    writer.writerow([datetime.datetime.now().strftime('%Y-%m-%dT%H:%MZ'),  sec_to_mmss(time.time() - start),
                     sec_to_mmss(time.time() - cp)])


def substring(str, len):
    if str is not None:
        return str[:len]


def chunks(data, size=10000):
    it = iter(data)
    for i in range(0, len(data), size):
        yield {k: data[k] for k in islice(it, size)}
