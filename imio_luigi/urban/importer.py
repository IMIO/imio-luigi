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
    
    def _get_url(self, data):
        """Construct POST url based on type"""
        folder = config[data["@type"]]["folder"]
        return f"{self.url}/{folder}"

    def _get_actual_references(self):
        global COMPLETE_REFERENCES
        result = requests.get(
            f"{self.url}/@@search"
            auth=(self.login, self.password),
            params={
                "object_provides": "Products.urban.interfaces.IBaseAllBuildLicence",
                "b_size": 29999,
                "metadata_fields": "getReference",
            },
            headers={"Accept": "application/json"},
        )
        if result.status_code == 200:
            for l in result["items"]:
                COMPLETE_REFERENCES.append(l["getReference"])

    def run(self):
        self._get_actual_references()
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
        global COMPLETE_REFERENCES
        if self.data["reference"] in COMPLETE_REFERENCES:
            return True
        r = self.test_complete()
        result = r.json()["items_total"] >= 1
        if result is True:
            COMPLETE_REFERENCES.append(self.data["reference"])
        return result
