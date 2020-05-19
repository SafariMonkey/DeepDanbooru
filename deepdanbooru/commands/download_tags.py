import os
import time

import requests
import psycopg2

import deepdanbooru as dd


def download_category_tags(category, minimum_post_count, limit, page_size=1000, order='count'):
    category_to_index = {
        'general': 0,
        'artist': 1,
        'copyright': 3,
        'character': 4
    }

    gold_only_tags = ['loli', 'shota', 'toddlercon']

    if category not in category_to_index:
        raise Exception(f'Not supported category : {category}')

    category_index = category_to_index[category]

    parameters = {
        'limit': page_size,
        'page': 1,
        'search[order]': order,
        'search[category]': category_index
    }

    request_url = 'https://danbooru.donmai.us/tags.json'

    tags = set()

    while True:
        response = requests.get(request_url, params=parameters)
        response_json = response.json()

        response_tags = [tag_json['name']
                         for tag_json in response_json if tag_json['post_count'] >= minimum_post_count]

        if not response_tags:
            break

        is_full = False

        for tag in response_tags:
            if tag in gold_only_tags:
                continue

            tags.add(tag)

            if len(tags) >= limit:
                is_full = True
                break

        if is_full:
            break
        else:
            parameters['page'] += 1

    return tags


def download_tags(project_path, limit, minimum_post_count, is_overwrite):
    project_context_path = os.path.join(project_path, 'project.json')
    project_context = dd.io.deserialize_from_json(project_context_path)
    if project_context['source'] not in ['danbooru']:
        raise Exception('download-tags is only available on danbooru projects')

    print(
        f'Start downloading tags ... (limit:{limit}, minimum_post_count:{minimum_post_count})')

    log = {
        'database': 'danbooru',
        'date': time.strftime("%Y/%m/%d %H:%M:%S"),
        'limit': limit,
        'minimum_post_count': minimum_post_count
    }

    system_tags = [
        'rating:safe',
        'rating:questionable',
        'rating:explicit',
        # 'score:very_bad',
        # 'score:bad',
        # 'score:average',
        # 'score:good',
        # 'score:very_good',
    ]

    category_definitions = [
        {
            'category_name': 'General',
            'category': 'general',
            'path': os.path.join(project_path, 'tags-general.txt'),
        },
        # {
        #    'category_name': 'Artist',
        #    'category': 'artist',
        #    'path': os.path.join(path, 'tags-artist.txt'),
        # },
        # {
        #    'category_name': 'Copyright',
        #    'category': 'copyright',
        #    'path': os.path.join(path, 'tags-copyright.txt'),
        # },
        {
            'category_name': 'Character',
            'category': 'character',
            'path': os.path.join(project_path, 'tags-character.txt'),
        },
    ]

    all_tags_path = os.path.join(project_path, 'tags.txt')

    if not is_overwrite and os.path.exists(all_tags_path):
        raise Exception(f'Tags file is already exists : {all_tags_path}')

    dd.io.try_create_directory(os.path.dirname(all_tags_path))
    dd.io.serialize_as_json(
        log, os.path.join(project_path, 'tags_log.json'))

    categories_for_web = []
    categories_for_web_path = os.path.join(project_path, 'categories.json')
    tag_start_index = 0

    total_tags_count = 0

    with open(all_tags_path, 'w') as all_tags_stream:
        for category_definition in category_definitions:
            category = category_definition['category']
            category_tags_path = category_definition['path']

            print(f'{category} tags are downloading ...')
            tags = download_category_tags(category, minimum_post_count, limit)

            tags = dd.extra.natural_sorted(tags)
            tag_count = len(tags)
            if tag_count == 0:
                print(f'{category} tags are not exists.')
                continue
            else:
                print(f'{tag_count} tags are downloaded.')

            with open(category_tags_path, 'w') as category_tags_stream:
                for tag in tags:
                    category_tags_stream.write(f'{tag}\n')
                    all_tags_stream.write(f'{tag}\n')

            categories_for_web.append(
                {'name': category_definition['category_name'], 'start_index': tag_start_index})

            tag_start_index += len(tags)
            total_tags_count += tag_count

        for tag in system_tags:
            all_tags_stream.write(f'{tag}\n')

        categories_for_web.append(
            {'name': 'System', 'start_index': total_tags_count}
        )

    dd.io.serialize_as_json(categories_for_web, categories_for_web_path)

    print(f'Total {total_tags_count} tags are downloaded.')

    print('All processes are complete.')

def derpi_import_tags(project_path, postgres_uri, limit, minimum_post_count, is_overwrite):
    project_context_path = os.path.join(project_path, 'project.json')
    project_context = dd.io.deserialize_from_json(project_context_path)
    if project_context['source'] not in ['derpibooru']:
        raise Exception('download-tags is only available on derpibooru projects')

    all_tags_path = os.path.join(project_path, 'tags.txt')
    
    if not is_overwrite and os.path.exists(all_tags_path):
        raise Exception(f'Tags file is already exists : {all_tags_path}')

    log = {
        'database': 'derpibooru',
        'date': time.strftime("%Y/%m/%d %H:%M:%S"),
        'limit': limit,
        'minimum_post_count': minimum_post_count
    }

    dd.io.try_create_directory(os.path.dirname(all_tags_path))
    dd.io.serialize_as_json(
        log, os.path.join(project_path, 'tags_log.json'))

    print(
        f'Start processing tags ... (limit:{limit}, minimum_post_count:{minimum_post_count})')

    connection = psycopg2.connect(postgres_uri)
    cursor = connection.cursor()

    category_definitions = [
        {
            'category_name': 'Character',
            'category': 'character',
            'path': os.path.join(project_path, 'tags-character.txt'),
        },
        # {
        #     'category_name': 'Content (fanmade)',
        #     'category': 'content-fanmade',
        #     'path': os.path.join(project_path, 'tags-content-fanmade.txt'),
        # },
        # {
        #     'category_name': 'Content (official)',
        #     'category': 'content-official',
        #     'path': os.path.join(project_path, 'tags-content-official.txt'),
        # },
        # {
        #     'category_name': 'Error',
        #     'category': 'error',
        #     'path': os.path.join(project_path, 'tags-error.txt'),
        # },
        # {
        #     'category_name': 'OC',
        #     'category': 'oc',
        #     'path': os.path.join(project_path, 'tags-oc.txt'),
        # },
        # {
        #     'category_name': 'Origin',
        #     'category': 'origin',
        #     'path': os.path.join(project_path, 'tags-origin.txt'),
        # },
        # {
        #     'category_name': 'Rating',
        #     'category': 'rating',
        #     'path': os.path.join(project_path, 'tags-rating.txt'),
        # },
        # {
        #     'category_name': 'Species',
        #     'category': 'species',
        #     'path': os.path.join(project_path, 'tags-species.txt'),
        # },
        # {
        #     'category_name': 'Spoiler',
        #     'category': 'spoiler',
        #     'path': os.path.join(project_path, 'tags-spoiler.txt'),
        # },
        # {
        #     'category_name': 'Other',
        #     'category': None,
        #     'path': os.path.join(project_path, 'tags-other.txt'),
        # },
    ]

    categories_for_web = []
    categories_for_web_path = os.path.join(project_path, 'categories.json')
    tag_start_index = 0

    total_tags_count = 0

    with open(all_tags_path, 'w') as all_tags_stream:
        for category_definition in category_definitions:
            category = category_definition['category']
            category_tags_path = category_definition['path']

            print(f'{category} tags are being loaded ...')

            cursor.execute(
                """
                    SELECT t.name, count(it.image_id) FROM tags t
                    JOIN image_taggings it ON it.tag_id = t.id
                    WHERE t.category = %(category)s
                        OR (t.category IS NULL AND %(category)s IS NULL)
                    GROUP BY t.name 
                    HAVING count(it.image_id) > %(post_count)s
                    ORDER BY count(it.image_id) DESC
                    LIMIT %(limit)s
                """,
                {'category': category, 'post_count': minimum_post_count,
                 'limit': limit})

            tags = [row[0] for row in cursor]

            tags = dd.extra.natural_sorted(tags)
            tag_count = len(tags)
            if tag_count == 0:
                print(f'{category} tags not found matching criteria.')
                continue
            else:
                print(f'{tag_count} tags have been processed.')

            with open(category_tags_path, 'w') as category_tags_stream:
                for tag in tags:
                    category_tags_stream.write(f'{tag}\n')
                    all_tags_stream.write(f'{tag}\n')

            categories_for_web.append(
                {'name': category_definition['category_name'], 'start_index': tag_start_index})

            tag_start_index += len(tags)
            total_tags_count += tag_count

        categories_for_web.append(
            {'name': 'System', 'start_index': total_tags_count}
        )

    dd.io.serialize_as_json(categories_for_web, categories_for_web_path)

