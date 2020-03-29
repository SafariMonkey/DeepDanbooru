import os
import psycopg2


def load_tags(tags_path):
    with open(tags_path, 'r') as tags_stream:
        tags = [tag for tag in (tag.strip() for tag in tags_stream) if tag]
        return tags


def load_image_records(database_uri, images_path, minimum_tag_count):
    if not os.path.exists(images_path):
        raise Exception(f'Image path does not exist : {images_path}')

    connection = psycopg2.connect(database_uri)

    cursor = connection.cursor()

    image_folder_path = os.path.join(os.path.dirname(images_path), 'images')

    cursor.execute(
        "SELECT s.* FROM (SELECT image_sha512_hash AS sha512, image_format as file_ext, string_agg(t.tag_id::text, ',') as tag_string, COUNT(t.tag_id) AS tag_count FROM images JOIN image_taggings t ON t.image_id = id WHERE (image_format = 'png' OR image_format = 'jpg' OR image_format = 'jpeg') AND image_sha512_hash IS NOT NULL GROUP BY id ORDER BY id) s WHERE s.tag_count >= %s",
        (minimum_tag_count,))

    colnames = [desc[0] for desc in cursor.description]
    col_to_index = {name: col for col, name in enumerate(colnames)}

    rows = cursor.fetchall()

    image_records = []

    for row in rows:
        sha512 = row[col_to_index['sha512']]
        extension = row[col_to_index['file_ext']]
        image_path = os.path.join(
            image_folder_path, sha512[0:2], f'{sha512}.{extension}')
        tag_string = row[col_to_index['tag_string']]

        image_records.append((image_path, tag_string))

    connection.close()

    return image_records
