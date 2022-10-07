import csv
import gzip
import io
import time
import orjson
import psycopg2.extras

from db_management import connect, create_tables, add_constraints
from id_generator import get_annotation_id, get_hashtag_id, get_conversation_hashtag_id, get_link_id, get_cann_id, \
    get_conversation_reference_id
from utility import get_json_field, substring, get_nested_json_field, replace_null_chars, chunks, csv_write_info

# there is only few unique domains, it is worth keeping ids in memory,
# not trying to insert and let sql handle conflicts
domain_ids = set()


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


inserted_authors = set()


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


def create_author(conversation, cur):
    if conversation["author_id"] not in inserted_authors:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO authors VALUES %s ON CONFLICT DO NOTHING;
                """, [(
            conversation["author_id"],
            None,
            None,
            None,
            None,
            None,
            None,
            None
        )])
        inserted_authors.add(conversation["author_id"])
    return conversation["author_id"]


def insert_conversations(conversations, cur):
    psycopg2.extras.execute_values(cur, """
        INSERT INTO conversations VALUES %s ON CONFLICT DO NOTHING;
            """, [(
        conversation["id"],
        create_author(conversation, cur),
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


def insert_annotations(annotations_dict, cur):
    annotations_arr = []
    for conversation_id, annotations in annotations_dict.items():
        for annotation in annotations:
            annotations_arr.append((
                get_annotation_id(),
                conversation_id,
                annotation["normalized_text"],
                annotation["type"],
                annotation["probability"]
            ))
    psycopg2.extras.execute_values(cur, """
        INSERT INTO annotations VALUES %s ON CONFLICT DO NOTHING;
            """, annotations_arr)


def insert_links(links_dict, cur):
    links_arr = []
    for conversation_id, urls in links_dict.items():
        for url in urls:
            links_arr.append((
                get_link_id(),
                conversation_id,
                get_json_field("expanded_url", url)[:2048],
                get_json_field("title", url),
                get_json_field("description", url)
            ))
    psycopg2.extras.execute_values(cur, """
        INSERT INTO links VALUES %s ON CONFLICT DO NOTHING;
            """, links_arr)


inserted_hashtags = {}


def insert_hashtags(hashtags_dict, cur):
    hashtags_arr = []
    conversation_hashtags = []
    global inserted_hashtags
    for conversation_id, hashtags in hashtags_dict.items():
        for hashtag in hashtags:
            if get_json_field("tag", hashtag) not in inserted_hashtags.keys():
                hashtag_id = get_hashtag_id()
                hashtags_arr.append({"conversation_id": conversation_id,
                                     "id": hashtag_id,
                                     "tag": get_json_field("tag", hashtag),
                                     }
                                    )
                inserted_hashtags[get_json_field("tag", hashtag)] = hashtag_id
            conversation_hashtags.append({"conversation_id": conversation_id,
                                          "id": inserted_hashtags[get_json_field("tag", hashtag)],
                                          })
    psycopg2.extras.execute_values(cur, """
        INSERT INTO hashtags VALUES %s ON CONFLICT DO NOTHING;
            """, [(
        hashtag["id"],
        hashtag["tag"]
    ) for hashtag in hashtags_arr])

    psycopg2.extras.execute_values(cur, """
           INSERT INTO conversation_hashtags VALUES %s ON CONFLICT DO NOTHING;
               """, [(
        get_conversation_hashtag_id(),
        hashtag["conversation_id"],
        hashtag["id"]
    ) for hashtag in conversation_hashtags])


references_dict_end = {}


def insert_references(references_dict, existing_conversation_ids, cur, end=False):
    global references_dict_end
    csv_file_like_object = io.StringIO()
    for conversation_id, references in references_dict.items():
        for reference in references:
            if get_json_field("id", reference) in existing_conversation_ids:
                csv_file_like_object.write('|'.join((
                    str(get_conversation_reference_id()),
                    str(conversation_id),
                    str(get_json_field("id", reference)),
                    get_json_field("type", reference)
                )) + '\n')
            elif end is False:
                if conversation_id not in references_dict_end:
                    references_dict_end[conversation_id] = [reference]
                else:
                    references_dict_end[conversation_id].append(reference)
    csv_file_like_object.seek(0)
    cur.copy_from(csv_file_like_object, 'conversation_references', sep='|')


def process_authors(cur, start, writer):
    cp0 = time.time()
    with gzip.open('authors.jsonl.gz', 'rt') as f:
        authors = []
        x = 0
        for line in f:
            x += 1
            author = orjson.loads(line)
            author['name'] = replace_null_chars(author['name'])
            author['username'] = replace_null_chars(author['username'])
            author['description'] = replace_null_chars(author['description'])
            authors.append(author)
            inserted_authors.add(author["id"])

            if x % 10000 == 0:
                insert_authors(authors, cur)
                authors = []
                csv_write_info(writer, start, cp0)
                cp0 = time.time()
        # insert remaining authors
        insert_authors(authors, cur)
        csv_write_info(writer, start, cp0)


def process_conversations(cur, start, writer):
    existing_conversation_ids = set()
    cp0 = time.time()
    x = 0
    with gzip.open('conversations.jsonl.gz', 'rt') as f:
        conversations = []
        context_annotations_dict = {}
        annotations_dict = {}
        links_dict = {}
        hashtag_dict = {}
        references_dict = {}
        for line in f:
            x += 1
            conversation = orjson.loads(line)
            if conversation["id"] in existing_conversation_ids:
                continue
            conversations.append(conversation)
            existing_conversation_ids.add(conversation["id"])
            if "context_annotations" in conversation:
                context_annotations_dict[conversation["id"]] = conversation["context_annotations"]
            if "entities" in conversation:
                if "annotations" in conversation["entities"]:
                    annotations_dict[conversation["id"]] = conversation["entities"]["annotations"]
                if "urls" in conversation["entities"]:
                    links_dict[conversation["id"]] = conversation["entities"]["urls"]
                if "hashtags" in conversation["entities"]:
                    hashtag_dict[conversation["id"]] = conversation["entities"]["hashtags"]
            if "referenced_tweets" in conversation:
                references_dict[conversation["id"]] = conversation["referenced_tweets"]

            if x % 10000 == 0:
                insert_conversations(conversations, cur)
                insert_annotations(annotations_dict, cur)
                insert_context_annotations(context_annotations_dict, cur)
                insert_links(links_dict, cur)
                insert_hashtags(hashtag_dict, cur)
                insert_references(references_dict, existing_conversation_ids, cur)
                annotations_dict = {}
                links_dict = {}
                context_annotations_dict = {}
                hashtag_dict = {}
                references_dict = {}
                conversations = []
                csv_write_info(writer, start, cp0)
                cp0 = time.time()

        # call inserts one more time for remaining records
        insert_conversations(conversations, cur)
        insert_annotations(annotations_dict, cur)
        insert_context_annotations(context_annotations_dict, cur)
        insert_links(links_dict, cur)
        insert_hashtags(hashtag_dict, cur)
        insert_references(references_dict, existing_conversation_ids, cur)
        csv_write_info(writer, start, cp0)
        cp0 = time.time()

        # try to insert references that could not be inserted before
        for references_chunk in chunks(references_dict_end):
            insert_references(references_chunk, existing_conversation_ids, cur, end=True)
            csv_write_info(writer, start, cp0)
            cp0 = time.time()


if __name__ == "__main__":
    f = open('out.csv', 'w', newline='')
    writer = csv.writer(f, delimiter=';')
    conn = connect()
    conn.autocommit = True
    cur = conn.cursor()
    create_tables(cur)
    start = time.time()
    process_authors(cur, start, writer)
    process_conversations(cur, start, writer)
    cp = time.time()
    add_constraints(cur)
    csv_write_info(writer, start, cp)
    f.close()
    end = time.time()
    print(end - start)
