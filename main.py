import gzip
import time
import orjson
import psycopg2.extras

from db_management import connect, create_tables
from id_generator import get_annotation_id, get_hashtag_id, get_conversation_hashtag_id, get_link_id, get_cann_id, \
    get_conversation_reference_id
from utility import get_json_field, substring, get_nested_json_field


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
        91305838,  # todo: change (hold a dict with authorids)
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
                get_json_field("url", url),
                get_json_field("title", url),
                get_json_field("description", url)
            ))
    psycopg2.extras.execute_values(cur, """
        INSERT INTO links VALUES %s ON CONFLICT DO NOTHING;
            """, links_arr)  # TODO: EXPANDED_URL?


inserted_hashtags = set()


def insert_hashtags(hashtags_dict, cur):
    hashtags_arr = []
    global inserted_hashtags
    for conversation_id, hashtags in hashtags_dict.items():
        for hashtag in hashtags:
            if get_json_field("tag", hashtag) not in inserted_hashtags:
                hashtags_arr.append({"conversation_id": conversation_id,
                                     "id": get_hashtag_id(),
                                     "tag": get_json_field("tag", hashtag),
                                     }
                                    )
                inserted_hashtags.add(get_json_field("tag", hashtag))
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
    ) for hashtag in hashtags_arr])


def insert_references(references_dict, existing_conversation_ids, cur):
    references_arr = []
    for conversation_id, references in references_dict.items():
        for reference in references:
            if get_json_field("id", reference) in existing_conversation_ids:
                references_arr.append((
                    get_conversation_reference_id(),
                    conversation_id,
                    get_json_field("id", reference),
                    get_json_field("type", reference)
                ))
    psycopg2.extras.execute_values(cur, """
        INSERT INTO conversation_references VALUES %s ON CONFLICT DO NOTHING;
            """, references_arr)


def process_authors():
    pass


def process_conversations():
    pass


if __name__ == "__main__":
    conn = connect()
    cur = conn.cursor()
    create_tables(cur)
    x = 0
    start = time.time()
    cp0 = time.time()
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
    existing_conversation_ids = set()
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

            if x % 50000 == 0:
                insert_conversations(conversations, cur)
                insert_annotations(annotations_dict, cur)
                insert_context_annotations(context_annotations_dict, cur)
                insert_links(links_dict, cur)
                insert_hashtags(hashtag_dict, cur)
                annotations_dict = {}
                links_dict = {}
                context_annotations_dict = {}
                hashtag_dict = {}
                conversations = []
                print(x)
                print(time.time() - cp0)
                cp0 = time.time()

            if x == 1000000:
                break
        # call inserts one more time for remaining records
        insert_conversations(conversations, cur)
        insert_annotations(annotations_dict, cur)
        insert_context_annotations(context_annotations_dict, cur)
        insert_links(links_dict, cur)
        insert_hashtags(hashtag_dict, cur)
        insert_references(references_dict, existing_conversation_ids, cur)  # TODO: uncomment

    end = time.time()
    print(end - start)
