import os
from typing import Any, Iterable, List, Tuple, Union

import six
import tensorflow as tf
import tornado.ioloop
import tornado.web
import requests
import io
import csv

import deepdanbooru as dd

from deepdanbooru.commands.evaluate import load_model, evaluate_image_raw


class MainHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        print("setting headers")
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def initialize(self, model, tags, tags_metadata, default_threshold):
        self.model = model
        self.tags = tags
        self.default_threshold = default_threshold

        if tags_metadata is not None:
            self.tags_metadata = {}
            for i, tag in enumerate(tags):
                metadata = tags_metadata[i]
                if metadata[1] != tag:
                    raise Exception("Broken tag metadata at index {}: {} != {}"
                                    .format(i, metadata[1], tag))
                self.tags_metadata[tag] = metadata
        else:
            self.tags_metadata = None

    def get(self):
        if (file := self.get_query_argument("file", default=None)) != None:
            data_handle = file
        elif (url := self.get_query_argument("url", default=None)) != None:
            resp = requests.get(url, stream=True)
            data_handle = io.BytesIO(resp.content)
        else:
            self.set_status(400)
            self.write({"message": "'file' or 'url' must be specified"})
            return

        threshold = float(self.get_query_argument("threshold", default=self.default_threshold))

        y = evaluate_image_raw(data_handle, self.model)

        def results_gen():
            for i, tag in enumerate(self.tags):
                confidence = float(y[i])
                if confidence < threshold:
                    continue
                result_item = {"tag_name": tag, "confidence": confidence}
                if self.tags_metadata is not None:
                    (tag_id, _, tag_category, tag_slug, image_count) = self.tags_metadata[tag]
                    result_item.update({
                        "tag_id": tag_id,
                        "tag_category": tag_category,
                        "tag_slug": tag_slug,
                        "image_count": image_count,
                    })

                yield result_item

        results = sorted(results_gen(), key=lambda item: item["confidence"], reverse=True)

        self.write({"matching_tags": results})
        self.write("\n")

    def options(self):
        print("recieved options request")
        # no body
        self.set_status(204)
        self.finish()


def make_app(model, tags, tags_metadata, default_threshold):
    return tornado.web.Application([
        (r"/evaluate", MainHandler, dict(
            model=model, tags=tags, tags_metadata=tags_metadata,
            default_threshold=default_threshold,
        )),
    ])


def load_tags_metadata(tags_metadata_path):
    with open(tags_metadata_path, 'r', newline='') as tags_metadata_stream:
        reader = csv.reader(tags_metadata_stream)
        return list(reader)


def serve_model(port, project_path, model_path, tags_path, tags_metadata_path,
                default_threshold, allow_gpu, compile_model,
                verbose):
    if not allow_gpu:
        os.environ['CUDA_VISIBLE_DEVICES'] = '-1'

    model, tags, tags_metadata = load_model(project_path, model_path, tags_path, compile_model, verbose,
                                            tags_metadata_path=tags_metadata_path)
    app = make_app(model, tags, tags_metadata, default_threshold)
    app.listen(port)
    tornado.ioloop.IOLoop.current().start()
