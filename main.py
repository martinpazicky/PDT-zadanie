import gzip
import io
import json
import time
from typing import Optional, Any

import orjson
import psycopg2.extras

from db_management import connect, create_tables
i = 0
csv_file_like_object = io.StringIO()
csv_file_like_object2 = io.StringIO()


def replace_null_chars(str):
    return str.replace("\x00", "\uFFFD")


def get_json_field(key, record):
    if key in record:
        return record[key]

def get_nested_json_field(key1, key2, record):
    if key1 in record:
        if key2 in record[key1]:
            return record[key1][key2]

def clean_csv_value(value: Optional[Any]) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')



domain_ids = {''}
entity_ids = {''}


def create_context_annotations_csv_objects(record, cur):
    if "context_annotations" in record:
        for cann in record["context_annotations"]:
            domain = get_json_field("domain", cann)
            entity = get_json_field("entity", cann)
            # insert into context_domains
            global csv_file_like_object
            if domain['id'] not in domain_ids:
                csv_file_like_object.write('|'.join(map(clean_csv_value, (
                    domain['id'],
                    get_json_field("name", domain),
                    get_json_field("description", domain)
                ))) + '\n')
                domain_ids.add(domain['id'])
            # insert into context_entities
            # if entity['id'] not in entity_ids:
            #     csv_file_like_object2.write('|'.join(map(clean_csv_value, (
            #         entity['id'],
            #         get_json_field("name", entity),
            #         get_json_field("description", entity)
            #     ))) + '\n')
            #     entity_ids.add(entity['id'])
            # print(domain['id'])
            # print(get_json_field("name", domain))
            # print(get_json_field("description", domain))
            # # insert into context_entities
            # print(entity['id'])
            # print(get_json_field("name", entity))
            # print(get_json_field("description", entity))
            # # insert into context_annotations
            # global i
            # print(record['id'])
            # print(domain['id'])
            # print(entity['id'])


csv_authors = io.StringIO()
def create_authors_csv_object(authors, cur):
    global i
    i += 1
    psycopg2.extras.execute_values(cur, """
        INSERT INTO authors VALUES %s ON CONFLICT DO NOTHING;
            """, [(
        author["id"],
        get_json_field("name", author),
        get_json_field("username", author),
        get_json_field("description", author),
        get_nested_json_field("public_metrics", "followers_count", author),
        get_nested_json_field("public_metrics", "following_count", author),
        get_nested_json_field("public_metrics", "tweet_count", author),
        get_nested_json_field("public_metrics", "listed_count", author)
    ) for author in authors])


if __name__ == "__main__":

    conn = connect()
    cur = conn.cursor()
    create_tables(cur)
    records = []
    x = 0
    start = time.time()
    with gzip.open('authors.jsonl.gz', 'rt') as f:
        authors = []
        for line in f:
            x += 1
            author = orjson.loads(line)
            author['name'] = replace_null_chars(author['name'])
            author['username'] = replace_null_chars(author['username'])
            author['description'] = replace_null_chars(author['description'])
            authors.append(author)

            if x % 1000000 == 0:
                print(x)
                create_authors_csv_object(authors, cur)

            # author["username"] = author["username"][:22]
    # with gzip.open('conversations.jsonl.gz', 'rt') as f:
    #     for line in f:
    #         x += 1
    #         if x > 1000000:
    #             break
    #         print(x)
    #         record = orjson.loads(line)
    #         create_context_annotations_csv_objects(record, cur)
    # csv_file_like_object.seek(0)
    # cur.copy_from(csv_file_like_object, 'context_domains', sep='|')
    # csv_file_like_object2.seek(0)
    # cur.copy_from(csv_file_like_object2, 'context_entities', sep='|')
    #
    # cur.execute("COMMIT")
    end = time.time()
    print(end - start)

