def replace_null_chars(str):
    return str.replace("\x00", "\uFFFD")


def get_json_field(key, record):
    if key in record:
        return record[key]


def get_nested_json_field(key1, key2, record):
    if key1 in record:
        if key2 in record[key1]:
            return record[key1][key2]


def substring(str, len):
    if str is not None:
        return str[:len]