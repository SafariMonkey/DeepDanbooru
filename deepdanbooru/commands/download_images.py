import os
import time

import requests

import deepdanbooru as dd



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

    print(f'A total of {downloaded_images} images were downloaded.')

    print('All processes are complete.')
