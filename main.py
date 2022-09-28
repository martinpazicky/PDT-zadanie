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


def substring(str, len):
    if str is not None:
        return str[:len]


domain_ids = {''}  # there is only few unique domains, it is worth keeping ids in memory,
                   # not trying to insert and let sql handle conflicts

context_annotations_id_counter = 0
def get_cann_id():
    global context_annotations_id_counter
    context_annotations_id_counter += 1
    return context_annotations_id_counter

def insert_context_annotations(context_annotations_dict, cur):
    global domain_ids
    domains = []
    entities = []
    context_annotations_parsed = []
    for conversation_id, context_annotations in context_annotations_dict.items():
        for cann in context_annotations:
            if cann['domain']['id'] not in domain_ids:
                domains.append(cann["domain"])
                domain_ids.add(cann['domain']['id'])
            entities.append(cann["entity"])
            context_annotations_parsed.append({"conversation_id": conversation_id,
                                               "domain_id": cann["domain"]["id"],
                                               "entity_id": cann["entity"]["id"]})
    if len(domains) > 0:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO context_domains VALUES %s ON CONFLICT DO NOTHING;
                    """, [(
            domain['id'],
            get_json_field("name", domain),
            get_json_field("description", domain)
        ) for domain in domains])

    psycopg2.extras.execute_values(cur, """
       INSERT INTO context_entities VALUES %s ON CONFLICT DO NOTHING;
                 """, [(
        entity['id'],
        substring(get_json_field("name", entity), 255),
        substring(get_json_field("description", entity), 255)
    ) for entity in entities])

    psycopg2.extras.execute_values(cur, """
           INSERT INTO context_annotations VALUES %s ON CONFLICT DO NOTHING;
                     """, [(
        get_cann_id(),
        cann["conversation_id"],
        cann["domain_id"],
        cann["entity_id"],
    ) for cann in context_annotations_parsed])



def insert_authors(authors, cur):
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


def insert_conversations(conversations, cur):
    psycopg2.extras.execute_values(cur, """
        INSERT INTO conversations VALUES %s ON CONFLICT DO NOTHING;
            """, [(
            conversation["id"],
            91305838,   # todo: change (hold a dict with authorids)
            get_json_field("text", conversation),
            get_json_field("possibly_sensitive", conversation),
            get_json_field("lang", conversation),
            get_json_field("source", conversation),
            get_nested_json_field("public_metrics", "retweet_count", conversation),
            get_nested_json_field("public_metrics", "reply_count", conversation),
            get_nested_json_field("public_metrics", "like_count", conversation),
            get_nested_json_field("public_metrics", "quote_count", conversation),
            get_json_field("created_at", conversation),
        ) for conversation in conversations])


if __name__ == "__main__":

    conn = connect()
    cur = conn.cursor()
    create_tables(cur)
    records = []
    x = 0
    start = time.time()
    # with gzip.open('authors.jsonl.gz', 'rt') as f:
    #     authors = []
    #     for line in f:
    #         x += 1
    #         author = orjson.loads(line)
    #         author['name'] = replace_null_chars(author['name'])
    #         author['username'] = replace_null_chars(author['username'])
    #         author['description'] = replace_null_chars(author['description'])
    #         authors.append(author)
    #
    #         if x % 10000 == 0:
    #             print(x)
    #             insert_authors(authors, cur)
    #             authors = []
    #     insert_authors(authors, cur)

    with gzip.open('conversations.jsonl.gz', 'rt') as f:
        conversations = []
        context_annotations_dict = {}
        for line in f:
            x += 1
            conversation = orjson.loads(line)
            conversations.append(conversation)
            if "context_annotations" in conversation:
                context_annotations_dict[conversation['id']] = conversation["context_annotations"]

            if x % 10000 == 0:
                print(x)
                insert_conversations(conversations, cur)
                insert_context_annotations(context_annotations_dict, cur)
                context_annotations_dict = {}
                conversations = []
            if x == 500000:
                break

        # insert_authors(authors, cur)


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

