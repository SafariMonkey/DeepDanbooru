import os
import psycopg2


def load_tags(tags_path):
    with open(tags_path, 'r') as tags_stream:
        tags = [tag for tag in (tag.strip() for tag in tags_stream) if tag]
        return tags


def load_image_records(database_uri, images_path, minimum_tag_count, limit=None, offset=0):
    if not os.path.exists(images_path):
        raise Exception(f'Image path does not exist : {images_path}')

    connection = psycopg2.connect(database_uri)

    cursor = connection.cursor()

    image_folder_path = os.path.join(os.path.dirname(images_path), 'images')

    cursor.execute(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS tagged_images AS SELECT i.id AS id, array_agg(t.id) as tag_ids, COUNT(t.id) AS tag_count, concat('https://derpicdn.net/img/view/', to_char(i.created_at, 'YYYY/fmMM/fmDD/'), i.id, '.', case when i.image_mime_type = 'image/svg+xml' then 'png' else lower(i.image_format) end) AS full_url FROM image_taggings JOIN images i ON i.id = image_id JOIN tags t on t.id = tag_id WHERE ((lower(i.image_format), i.image_mime_type) in (('png', 'image/png'), ('jpg', 'image/jpeg'), ('jpeg', 'image/jpeg'), ('svg', 'image/svg'))) GROUP BY i.id",
        (minimum_tag_count,))

    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS index_tagged_images_id ON tagged_images (id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS index_tagged_images_tag_count ON tagged_images (tag_count);")
    cursor.execute("CREATE INDEX IF NOT EXISTS index_tagged_images_on_tag_ids ON tagged_images USING GIN (tag_ids);")

    connection.commit()

    cursor.execute(
        "SELECT * FROM tagged_images WHERE tag_count >= %s ORDER BY id LIMIT %s OFFSET %s",
        (minimum_tag_count, limit, offset))

    colnames = [desc[0] for desc in cursor.description]
    col_to_index = {name: col for col, name in enumerate(colnames)}

    rows = cursor.fetchall()

    image_records = []

    for row in rows:
        image_id = row[col_to_index['id']]
        image_full_url = row[col_to_index['full_url']]
        extension = image_full_url.rsplit('.', 1)[1]
        image_path = os.path.join(
            image_folder_path, f'{image_id % 1000:0>3d}', f'{image_id:0>7d}.{extension}')
        tag_ids= row[col_to_index['tag_ids']]

        image_records.append((image_full_url, image_path, tag_ids))

    connection.close()

    return image_records
