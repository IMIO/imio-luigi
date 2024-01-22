# -*- coding: utf-8 -*-

from imio_luigi import core
from imio_luigi.urban.core import config
from timeit import default_timer as timer

import base64
import json
import logging
import luigi
import requests


logger = logging.getLogger("luigi-interface")

COMPLETE_REFERENCES = []


class GetFiles(core.WalkFS):
    task_namespace = "urban"
    extension = ".json"
    path = luigi.Parameter()
    url = luigi.Parameter()
    login = luigi.Parameter()
    password = luigi.Parameter()
    task_complete = False

    def _get_url(self, data):
        """Construct POST url based on type"""
        folder = config[data["@type"]]["folder"]
        return f"{self.url}/{folder}"

    def _get_actual_references(self):
        global COMPLETE_REFERENCES

        result = requests.get(
            f"{self.url}/@search",
            auth=(self.login, self.password),
            params={
                "object_provides": "Products.urban.interfaces.IUrbanBase",
                "b_size": 99999,
                "metadata_fields": "getReference",
            },
            headers={"Accept": "application/json"},
        )
        if result.status_code == 200:
            for l in result.json()["items"]:
                COMPLETE_REFERENCES.append(l["getReference"].upper().strip())

    def requires(self):
        global COMPLETE_REFERENCES

        self._get_actual_references()
        for fpath in self.filepaths:
            with open(fpath, "r") as f:
                content = json.load(f)
                if content["reference"].upper() in COMPLETE_REFERENCES:
                    continue
                yield RESTPost(
                    key=content[config[content["@type"]]["config"]["key"]],
                    data=content,
                    url=self._get_url(content),
                    login=self.login,
                    password=self.password,
                    search_on=config[content["@type"]]["config"]["search_on"]
                )

    def run(self):
        self.task_complete = True

    def complete(self):
        return self.task_complete


class RESTPost(core.PostRESTTask):
    task_namespace = "urban"
    key = luigi.Parameter()
    data = luigi.DictParameter()
    url = luigi.Parameter()
    login = luigi.Parameter()
    password = luigi.Parameter()
    search_on = luigi.Parameter()
    has_run = False

    @property
    def test_url(self):
        return f"{self.url}/@search"

    def run(self):
        self.output().request()
        self.has_run = True

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
        json_body["reference"] = json_body["reference"].upper().strip()
        json_body["disable_check_ref_format"] = True
        return json_body

    def complete(self):
        global COMPLETE_REFERENCES
        if self.has_run is False:
            return False
        if self.data["reference"].upper().strip() in COMPLETE_REFERENCES:
            return True
        r = self.test_complete()
        result = r.json()["items_total"] >= 1
        if result is True:
            COMPLETE_REFERENCES.append(self.data["reference"].upper().strip())
        return result
