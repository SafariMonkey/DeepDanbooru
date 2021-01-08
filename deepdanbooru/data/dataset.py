import os
import csv
import sqlite3


def load_tags(tags_path):
    with open(tags_path, 'r') as tags_stream:
        tags = [tag for tag in (tag.strip() for tag in tags_stream) if tag]
        return tags


def load_tags_metadata(tags_metadata_path):
    with open(tags_metadata_path, 'r', newline='') as tags_metadata_stream:
        reader = csv.reader(tags_metadata_stream)
        return list(reader)


def load_image_records(sqlite_path, minimum_tag_count):
    if not os.path.exists(sqlite_path):
        raise Exception(f'SQLite database is not exists : {sqlite_path}')

    try:
        connection = sqlite3.connect(sqlite_path)
    except Exception as e:
        import pdb; pdb.set_trace()
        pass
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    image_folder_path = os.path.join(os.path.dirname(sqlite_path), 'images')

    cursor.execute(
        "SELECT foldername, filename, extension, tag_string, download_url FROM posts WHERE (extension = 'png' OR extension = 'jpg' OR extension = 'jpeg') AND (tag_count_general >= ?) ORDER BY id",
        (minimum_tag_count,))

    rows = cursor.fetchall()

    image_records = []

    for row in rows:
        foldername = row['foldername']
        filename = row['filename']
        extension = row['extension']
        image_path = os.path.join(
            image_folder_path, foldername, f'{filename}.{extension}')
        tag_string = row['tag_string']
        download_url = row['download_url']

        image_records.append((image_path, tag_string, download_url))

    connection.close()

    return image_records
