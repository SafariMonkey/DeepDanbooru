import os
from typing import Any, Iterable, List, Tuple, Union

import six
import tensorflow as tf
import tornado.ioloop
import tornado.web
import requests
import io

import deepdanbooru as dd

from deepdanbooru.commands.evaluate import load_model, evaluate_image


class MainHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        print("setting headers")
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def initialize(self, model, tags, default_threshold):
        self.model = model
        self.tags = tags
        self.default_threshold = default_threshold

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

        results = []

        tags_gen = evaluate_image(data_handle, self.model, self.tags, threshold)
        for tag, score in sorted(tags_gen, key=lambda tag_score: tag_score[1], reverse=True):
            results.append({"tag": tag, "confidence": float(score)})

        self.write({"matching_tags": results})
        self.write("\n")

    def options(self):
        print("recieved options request")
        # no body
        self.set_status(204)
        self.finish()


def make_app(model, tags, default_threshold):
    return tornado.web.Application([
        (r"/evaluate", MainHandler, dict(
            model=model, tags=tags, default_threshold=default_threshold,
        )),
    ])


def serve_model(port, project_path, model_path, tags_path,
                default_threshold, allow_gpu, compile_model,
                verbose):
    if not allow_gpu:
        os.environ['CUDA_VISIBLE_DEVICES'] = '-1'

    model, tags = load_model(project_path, model_path, tags_path, compile_model, verbose)
    app = make_app(model, tags, default_threshold)
    app.listen(port)
    tornado.ioloop.IOLoop.current().start()
