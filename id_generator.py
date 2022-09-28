context_annotations_id_counter = 0
annotations_id_counter = 0
links_id_counter = 0
hashtags_id_counter = 0
conversation_hashtags_id_counter = 0
conversation_references_id_counter = 0


def get_cann_id():
    global context_annotations_id_counter
    context_annotations_id_counter += 1
    return context_annotations_id_counter


def get_annotation_id():
    global annotations_id_counter
    annotations_id_counter += 1
    return annotations_id_counter


def get_link_id():
    global links_id_counter
    links_id_counter += 1
    return links_id_counter


def get_hashtag_id():
    global hashtags_id_counter
    hashtags_id_counter += 1
    return hashtags_id_counter


def get_conversation_hashtag_id():
    global conversation_hashtags_id_counter
    conversation_hashtags_id_counter += 1
    return conversation_hashtags_id_counter


def get_conversation_reference_id():
    global conversation_references_id_counter
    conversation_references_id_counter += 1
    return conversation_references_id_counter
