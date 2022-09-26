import gzip
import json
import time

import psycopg2
import orjson


def connect():
    """ Connect to the PostgreSQL database server """
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(
            host="localhost",
            database="PDT-Z1",
            user="postgres",
            password="1234")
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def create_tables(cur):
    cur.execute("""
           DROP TABLE IF EXISTS authors CASCADE; 
           CREATE UNLOGGED TABLE authors (
               id                  int8 PRIMARY KEY,
               name                varchar(255),
               username            varchar(255),
               description         text,
               followers_count     int4,
               following_count     int4,
               tweet_count         int4,
               listed_count        int4
           );
           COMMIT;
       """)

    cur.execute("""
           DROP TABLE IF EXISTS conversations CASCADE;
           CREATE UNLOGGED TABLE conversations (
               id                  int8 PRIMARY KEY UNIQUE, 
               author_id           int8 references authors(id),
               content             text,
               possibly_sensitive  bool,
               language            varchar(3),
               source              text,
               retweet_count       int4,
               reply_count         int4,
               like_count          int4,
               quote_count         int4,
               created_at          timestamptz
           );
           COMMIT;
       """)

    cur.execute("""
           DROP TABLE IF EXISTS hashtags CASCADE;
           CREATE UNLOGGED TABLE hashtags (
               id                  int8 PRIMARY KEY,
               tag                 text UNIQUE
           );
           COMMIT;
       """)

    cur.execute("""
           DROP TABLE IF EXISTS conversation_hashtags;
           CREATE UNLOGGED TABLE conversation_hashtags (
               id                  int8 PRIMARY KEY,
               conversation_id     int8 references conversations(id),
               hashtag_id          int8 references hashtags(id)
           );
           COMMIT;
       """)

    cur.execute("""
           DROP TABLE IF EXISTS context_domains CASCADE;
           CREATE UNLOGGED TABLE context_domains (
               id                  int8 PRIMARY KEY,
               name                varchar(255),
               description         varchar(255)
           );
           COMMIT;
       """)

    cur.execute("""
           DROP TABLE IF EXISTS context_entities CASCADE;
           CREATE UNLOGGED TABLE context_entities (
               id                  int8 PRIMARY KEY,
               name                varchar(255),
               description         varchar(255)
           );
           COMMIT;
       """)

    cur.execute("""
           DROP TABLE IF EXISTS context_annotations;
           CREATE UNLOGGED TABLE context_annotations (
               id                  int8 PRIMARY KEY,
               conversation_id     int8 references conversations(id),
               context_domains_id  int8 references context_domains(id),
               context_entity      int8 references context_entities(id)
           );
           COMMIT;
       """)

    cur.execute("""
           DROP TABLE IF EXISTS annotations CASCADE;
           CREATE UNLOGGED TABLE annotations (
               id                  int8 PRIMARY KEY,
               conversation_id     int8 references conversations(id),
               value               text,
               type                text,
               probability         numeric(4,3)
           );
           COMMIT;
       """)

    cur.execute("""
           DROP TABLE IF EXISTS links CASCADE;
           CREATE UNLOGGED TABLE links (
               id                  int8 PRIMARY KEY,
               conversation_id     int8 references conversations(id),
               url                 varchar(2048),
               title               text,
               description         text
           );
           COMMIT;
       """)

    cur.execute("""
           DROP TABLE IF EXISTS conversation_references;
           CREATE UNLOGGED TABLE conversation_references (
               id                  int8 PRIMARY KEY,
               conversation_id     int8 references conversations(id),
               parent_id           int8 references conversations(id),
               type                varchar(20)
           );
           COMMIT;
       """)


if __name__ == "__main__":
    conn = connect()
    cur = conn.cursor()
    create_tables(cur)
    records = []
    x = 0
    start = time.time()
    with gzip.open('conversations.jsonl.gz', 'rt') as f:
        for line in f:
            x += 1
            if x > 100:
                break
            print(x)
            record = orjson.loads(line)
            print(record)
    end = time.time()
    print(end - start)
