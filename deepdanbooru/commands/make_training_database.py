import os
import sqlite3
import psycopg2
import psycopg2.extras


def make_training_database(source_format, source_uri, output_path, start_id, end_id,
                           use_deleted, chunk_size, overwrite, vacuum):
    '''
    Make sqlite database for training. Also add system tags.
    '''

    if source_uri == output_path:
        raise Exception('Source uri and output path is equal.')

    if os.path.exists(output_path):
        if overwrite:
            os.remove(output_path)
        else:
            raise Exception(f'{output_path} is already exists.')

    if source_format == 'danbooru':
        src = DanbooruSource(file_path=source_uri)
    elif source_format == 'derpibooru':
        src = DerpibooruSource(postgres_uri=source_uri)
    else:
        raise ValueError("Unhandled source format %s" % source_format)

    out = OutputDatabase(file_path=output_path)

    # Create output table
    print('Creating table ...')
    out.cursor.execute(f"""CREATE TABLE {out.table} (
        {out.id.column} INTEGER NOT NULL PRIMARY KEY,
        {out.foldername.column} TEXT,
        {out.filename.column} TEXT,
        {out.extension.column} TEXT,
        {out.download_url.column} TEXT,
        {out.tag_string.column} TEXT,
        {out.tag_count_general.column} INTEGER )""")
    out.connection.commit()
    print('Creating table is complete.')

    current_start_id = start_id

    while True:
        print(
            f'Fetching source rows ... ({current_start_id}~)')
        src.cursor.execute(
            f"""SELECT
                {src.id._as},{src.filename._as},{src.foldername._as},{src.extension._as},{src.download_url._as},{src.tag_string._as},{src.tag_count_general._as},{src.score._as},{src.deleted._as}
            FROM {src.from_clause} WHERE ({src.id.query} >= {src.placeholder}) AND {src.where_clause} GROUP BY {src.group_by_clause} ORDER BY {src.id.query} ASC LIMIT {src.placeholder}""",
            (current_start_id, chunk_size))

        rows = src.cursor.fetchall()

        if not rows:
            break

        insert_params = []

        for row in rows:
            post_id = row[src.id.column]
            download_url = row[src.download_url.column]
            foldername = row[src.foldername.column]
            filename = row[src.filename.column]
            extension = row[src.extension.column]
            tag_string = row[src.tag_string.column]
            general_tag_count = row[src.tag_count_general.column]
            # score = row[src.score.column]
            is_deleted = row[src.deleted.column]

            if post_id > end_id:
                break

            if is_deleted and not use_deleted:
                continue

            # if score < -6:
            #     tags += f' score:very_bad'
            # elif score >= -6 and score < 0:
            #     tags += f' score:bad'
            # elif score >= 0 and score < 7:
            #     tags += f' score:average'
            # elif score >= 7 and score < 13:
            #     tags += f' score:good'
            # elif score >= 13:
            #     tags += f' score:very_good'

            insert_params.append(
                (post_id, foldername, filename, extension, download_url, tag_string, general_tag_count))

        if insert_params:
            print('Inserting ...')
            out.cursor.executemany(
                f"""INSERT INTO {out.table} (
                {out.id.column},{out.foldername.column},{out.filename.column},{out.extension.column},{out.download_url.column},{out.tag_string.column},{out.tag_count_general.column})
                values (?, ?, ?, ?, ?, ?, ?)""", insert_params)
            out.connection.commit()

        current_start_id = rows[-1][src.id.column] + 1

        if current_start_id > end_id or len(rows) < chunk_size:
            break

    if vacuum:
        print('Vacuum ...')
        out.cursor.execute('vacuum')
        out.connection.commit()

    src.connection.close()
    out.connection.close()


class SqliteDatabase:
    def __init__(self, file_path):
        self.connection = sqlite3.connect(file_path)
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()
        self.placeholder = '?'


class PostgresDatabase:
    def __init__(self, postgres_uri):
        self.connection = psycopg2.connect(postgres_uri)
        self.cursor = self.connection.cursor(cursor_factory = psycopg2.extras.DictCursor)
        self.placeholder = '%s'


class QueryColumn:
    def __init__(self, column_name):
        self.column = column_name
        self.query = column_name

    @property
    def _as(self):
        return f'{self.query} AS {self.column}'


class TagDatabase:
    id = QueryColumn('id')
    foldername = QueryColumn('foldername')
    filename = QueryColumn('filename')
    extension = QueryColumn('extension')
    download_url = QueryColumn('download_url')
    tag_string = QueryColumn('tag_string')
    tag_count_general = QueryColumn('tag_count_general')


class SourceDatabase(TagDatabase):
    from_clause = 'posts'
    where_clause = 'true'
    group_by_clause = 'id'

    score = QueryColumn('score')
    deleted = QueryColumn('is_deleted')


class DanbooruSource(SqliteDatabase, SourceDatabase):
    def __init__(self, file_path):
        super().__init__(file_path)
        self.tag_delimiter = ' '
        self.foldername.query = 'left(md5, 2)'
        self.filename.query = 'md5'
        # concat rating tag with rest of tag string
        self.tag_string.query = f"""
        ltrim(
            tag_string
            ||
            case
                when rating = 's' then '{self.tag_delimiter}rating:safe'
                when rating = 'q' then '{self.tag_delimiter}rating:questionable'
                when rating = 'e' then '{self.tag_delimiter}rating:explicit'
                else ''
            end
            , '{self.tag_delimiter}'
        )
        """
        self.download_url.query = f'null'


class DerpibooruSource(PostgresDatabase, SourceDatabase):
    def __init__(self, postgres_uri):
        super().__init__(postgres_uri)
        self.from_clause = '''
        image_taggings
        JOIN images i ON i.id = image_id
        JOIN tags t ON t.id = tag_id
        '''
        self.where_clause = """
        ((lower(i.image_format), i.image_mime_type) in (
            ('png', 'image/png'),
            ('jpg', 'image/jpeg'),
            ('jpeg', 'image/jpeg'),
            ('svg', 'image/svg+xml')
        ))
        """
        self.group_by_clause = 'i.id'

        self.tag_delimiter = ','

        self.id.query = 'i.id'
        self.tag_count_general.query = 'count(t.id)'
        self.tag_string.query = f"string_agg(t.name, '{self.tag_delimiter}')"
        self.foldername.query = f"right(lpad(i.id::text, 7, '0'), 2)"
        self.filename.query = f"lpad(i.id::text, 7, '0')"
        self.extension.query = """
        case
            when i.image_mime_type = 'image/svg+xml' then 'png'
            else lower(i.image_format)
        end
        """
        self.download_url.query = f"""
            concat(
                'https://derpicdn.net/img/view/',
                to_char(i.created_at, 'YYYY/fmMM/fmDD/'),
                i.id,
                '.', 
                {self.extension.query}
            )
        """
        self.deleted.query = f'false'


class OutputDatabase(SqliteDatabase, TagDatabase):
    table = 'posts'
