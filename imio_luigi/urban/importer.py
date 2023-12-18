# -*- coding: utf-8 -*-

from imio_luigi import core
from imio_luigi.urban.core import config
from timeit import default_timer as timer

import base64
import json
import logging
import luigi


logger = logging.getLogger("luigi-interface")


class GetFiles(core.WalkFS):
    task_namespace = "urban"
    extension = ".json"
    path = luigi.Parameter()
    url = luigi.Parameter()
    login = luigi.Parameter()
    password = luigi.Parameter()
    
    def _get_url(self, data):
        """Construct POST url based on type"""
        folder = config[data["@type"]]["folder"]
        return f"{self.url}/{folder}"

    def run(self):
        for fpath in self.filepaths:
            with open(fpath, "r") as f:
                content = json.load(f)
                yield RESTPost(
                    key=content[config[content["@type"]]["config"]["key"]],
                    data=content,
                    url=self._get_url(content),
                    login=self.login,
                    password=self.password,
                    search_on=config[content["@type"]]["config"]["search_on"]
                )


class RESTPost(core.PostRESTTask):
    task_namespace = "urban"
    key = luigi.Parameter()
    data = luigi.DictParameter()
    url = luigi.Parameter()
    login = luigi.Parameter()
    password = luigi.Parameter()
    search_on = luigi.Parameter()

    @property
    def test_url(self):
        return f"{self.url}/@search"

    def run(self):
        result = self.output().request()

    def _fix_character(self, term, character):
        if character in term:
            joiner = '"{}"'.format(character)
            term = joiner.join(term.split(character))
        return term

    def _fix_key(self, key):
        key = self._fix_character(key, "(")
        key = self._fix_character(key, ")")
        return key

    @property
    def test_parameters(self):
        return {"portal_type": self.data["@type"], self.search_on: self._fix_key(self.key)}

    def _add_attachments(self, data):
        if "__children__" not in data:
            return data
        for child in data["__children__"]:
            if "file" not in child:
                continue
            with open(child["file"]["data"], "rb") as f:
                content = f.read()
                child["file"]["data"] = base64.b64encode(content).decode("utf8")
        return data

    @property
    def json_body(self):
        json_body = self._add_attachments(core.frozendict_to_dict(self.data))
        json_body["disable_check_ref_format"] = True
        return json_body

    def complete(self):
        r = self.test_complete()
        result = r.json()["items_total"] >= 1
        return result
