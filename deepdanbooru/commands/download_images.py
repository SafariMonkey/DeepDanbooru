import glob
import json
import logging
import os
import random
import requests
import shutil
import sys
import threading
import time
import traceback

from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import requests
import sqlite3

import deepdanbooru as dd


threadSessionHolder = threading.local()


class ImageFetchFailed(Exception):
    pass



def download_images(project_path, is_overwrite):
    project_context_path = os.path.join(project_path, 'project.json')
    project_context = dd.io.deserialize_from_json(project_context_path)
    if project_context['source'] not in ['derpibooru']:
        raise Exception('download-images is only available on derpibooru projects')
    
    sqlite_path = project_context['database_path']
    image_folder_path = os.path.join(os.path.dirname(sqlite_path), 'images')

    print(
        f'Start downloading images from URLs listed in {sqlite_path} to {image_folder_path}')

    dd.io.try_create_directory(image_folder_path)

    connection = sqlite3.connect(sqlite_path)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    cursor.execute("SELECT foldername, filename, extension, download_url FROM posts WHERE (extension = 'png' OR extension = 'jpg' OR extension = 'jpeg') ORDER BY id")

    setup_logging()

    processed_images = 0
    succeeded_images = 0
    try:
        while True:
            batch = cursor.fetchmany(1000)
            if not batch:
                break
            batch_data = []
            for row in batch:
                foldername = row['foldername']
                filename = row['filename']
                extension = row['extension']
                image_path = os.path.join(
                    image_folder_path, foldername, f'{filename}.{extension}')
                download_url = row['download_url']
                batch_data.append((download_url, image_path, is_overwrite))

            results = list(fetch_images_parallel(batch_data))
            succeeded = len(batch_data) - results.count(None)

            processed_images += len(batch_data)
            succeeded_images += succeeded

            logging.info(f"BATCH: attempted to fetch {len(batch_data)} images, {succeeded} succeeded")

            if free_space_left(image_folder_path) < 10_000*1024*1024:
                logging.warn(f"only {free_space_left(image_folder_path)//(1024*1024)}MB left, stopping")
                break
    except KeyboardInterrupt:
        print('Got KeyboardInterrupt, stopping. (This may take a few seconds.)')

    print(f'A total of {processed_images} images were processed for download, '
          f'of which {succeeded_images} images were successfully downloaded.')


def fetch_image_instrumented(image):
    return print_before(f"starting {image}",
        print_exc,
        rate_limit, 1.0, 8,
        print_success, f"finished {image}",
        download_image, *image
    )


# May be slightly slower than fetch_images_parallel, but more debuggable.
def fetch_images_sequential(images):
    results = []
    for image in images:
        results.append(fetch_image_instrumented(image))
    return results


# May or may not be faster than fetch_images_sequential (with Session reuse).
# When it is, we'll take what we can get.
def fetch_images_parallel(images, executor=None):
    if executor is None:
        with ProcessPoolExecutor(max_workers=10) as executor:
            return executor.map(fetch_image_instrumented, images)
    else:
        return executor.map(fetch_image_instrumented, images)


def download_image(url, path, is_overwrite):
    # So ideally, if is_overwrite is False, we don't want to download
    # the image if the file already exists. However, we don't get a
    # FileExistsError until we're inside the file open block.
    # (For this we cheat and use os.path.exists to avoid creating the file
    # and then failing to download it.) Additionally, the directory may not
    # exist, so we need to catch that case and create the directory and then
    # retry.

    if not is_overwrite and os.path.exists(path):
        # The file already exists, so just return
        return True

    local_session = thread_local_session()
    r = local_session.get(url, stream=True)
    if r.status_code != 200:
        raise ImageFetchFailed(
            "Fetching from URL {} to file {} failed: {}".format(
                url, path, r.status
            )
        )
    def write_file():
        with open(path, "wb" if is_overwrite else "xb") as f:
            for chunk in r:
                f.write(chunk)
    try:
        write_file()
    except FileNotFoundError:
        dd.io.try_create_directory(os.path.dirname(path))
        write_file()
    except FileExistsError:
        # unexpected as we already checked the existence earlier
        return True
    return True


def thread_local_session():
    if  getattr(threadSessionHolder, 'initialised', None) is None:
        threadSessionHolder.session = requests.Session()
        logging.debug("Made new session")
        threadSessionHolder.initialised = True
    return threadSessionHolder.session


def setup_logging():
    log_level = logging.INFO
    if os.getenv("DEBUG", "false") == "true":
        log_level = logging.DEBUG
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stderr,
        level=log_level,
    )


def print_success(message, f, *args, **kwargs):
    ret = f(*args, **kwargs)
    logging.debug(message)
    return ret


def print_before(message, f, *args, **kwargs):
    logging.debug(message)
    return f(*args, **kwargs)


def print_exc(f, *args, **kwargs):
    try:
        return f(*args, **kwargs)
    except Exception as e:
        formatted = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        logging.debug(f"Encountered error: {str(e)}: \n{formatted}")
        return None


def rate_limit(sleep, tries, f, *args, **kwargs):
    try:
        return f(*args, **kwargs)
    except Exception as e:
        if '429' in str(e) and tries > 0:
            random_sleep = sleep * 2 * random.random()
            logging.debug(f"backing off ~{sleep}s ({random_sleep:.2f}s)")
            time.sleep(random_sleep)
            return rate_limit(sleep * 2, tries - 1, f, *args, **kwargs)
        else:
            raise


def free_space_left(path):
    statvfs = os.statvfs(path)
    return statvfs.f_frsize * statvfs.f_bavail
