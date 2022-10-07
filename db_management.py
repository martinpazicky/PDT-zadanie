import psycopg2


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
           CREATE TABLE authors (
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
           CREATE  TABLE conversations (
               id                  int8 PRIMARY KEY UNIQUE, 
               author_id           int8,
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
           CREATE  TABLE hashtags (
               id                  int8 PRIMARY KEY,
               tag                 text UNIQUE
           );
           COMMIT;
       """)

    cur.execute("""
           DROP TABLE IF EXISTS conversation_hashtags;
           CREATE  TABLE conversation_hashtags (
               id                  int8 PRIMARY KEY,
               conversation_id     int8,
               hashtag_id          int8
           );
           COMMIT;
       """)

    cur.execute("""
           DROP TABLE IF EXISTS context_domains CASCADE;
           CREATE  TABLE context_domains (
               id                  int8 PRIMARY KEY,
               name                varchar(255),
               description         varchar(255)
           );
           COMMIT;
       """)

    cur.execute("""
           DROP TABLE IF EXISTS context_entities CASCADE;
           CREATE  TABLE context_entities (
               id                  int8 PRIMARY KEY,
               name                varchar(255),
               description         varchar(255)
           );
           COMMIT;
       """)

    cur.execute("""
           DROP TABLE IF EXISTS context_annotations;
           CREATE  TABLE context_annotations (
               id                  int8 PRIMARY KEY,
               conversation_id     int8,
               context_domain_id   int8,
               context_entity_id   int8 
           );
           COMMIT;
       """)

    cur.execute("""
           DROP TABLE IF EXISTS annotations CASCADE;
           CREATE  TABLE annotations (
               id                  int8 PRIMARY KEY,
               conversation_id     int8,
               value               text,
               type                text,
               probability         numeric(4,3)
           );
           COMMIT;
       """)

    cur.execute("""
           DROP TABLE IF EXISTS links CASCADE;
           CREATE  TABLE links (
               id                  int8 PRIMARY KEY,
               conversation_id     int8,
               url                 varchar(2048),
               title               text,
               description         text
           );
           COMMIT;
       """)

    cur.execute("""
           DROP TABLE IF EXISTS conversation_references;
           CREATE  TABLE conversation_references (
               id                  int8 PRIMARY KEY,
               conversation_id     int8,
               parent_id           int8,
               type                varchar(20)
           );
           COMMIT;
       """)


def add_constraints(cur):
    cur.execute("""
            ALTER TABLE conversations
            ADD CONSTRAINT fk_conversations_authors FOREIGN KEY (author_id) REFERENCES authors(id);
            ALTER TABLE conversation_hashtags
            ADD CONSTRAINT fk_conversation_hashtags_conversations FOREIGN KEY (conversation_id) REFERENCES conversations(id);
            ALTER TABLE conversation_hashtags
            ADD CONSTRAINT fk_conversation_hashtags_hashtags FOREIGN KEY (hashtag_id) REFERENCES hashtags(id);
            ALTER TABLE context_annotations
            ADD CONSTRAINT fk_context_annotations_conversations FOREIGN KEY (conversation_id) REFERENCES conversations(id);
            ALTER TABLE context_annotations
            ADD CONSTRAINT fk_context_annotations_context_domains FOREIGN KEY (context_domain_id) REFERENCES context_domains(id);
            ALTER TABLE context_annotations
            ADD CONSTRAINT fk_context_annotations_context_entities FOREIGN KEY (context_entity_id) REFERENCES context_entities(id);
            ALTER TABLE annotations
            ADD CONSTRAINT fk_annotations_conversations FOREIGN KEY (conversation_id) REFERENCES conversations(id);
            ALTER TABLE links
            ADD CONSTRAINT fk_links_conversations FOREIGN KEY (conversation_id) REFERENCES conversations(id);
            ALTER TABLE conversation_references
            ADD CONSTRAINT fk_conversations_references_conversations FOREIGN KEY (conversation_id) REFERENCES conversations(id);
            ALTER TABLE conversation_references
            ADD CONSTRAINT fk_conversations_references_parents FOREIGN KEY (parent_id) REFERENCES conversations(id);
            COMMIT;
           """)
