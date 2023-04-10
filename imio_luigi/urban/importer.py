# -*- coding: utf-8 -*-

from imio_luigi import core

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

    def run(self):
        for fpath in self.filepaths:
            with open(fpath, "r") as f:
                content = json.load(f)
                yield RESTPost(
                    key=content["reference"],
                    data=content,
                    url=self.url,
                    login=self.login,
                    password=self.password,
                )


class RESTPost(core.PostRESTTask):
    task_namespace = "urban"
    key = luigi.Parameter()
    data = luigi.DictParameter()
    url = luigi.Parameter()
    login = luigi.Parameter()
    password = luigi.Parameter()

    @property
    def test_url(self):
        return f"{self.url}/@search"

    def run(self):
        result = self.output().request()

    @property
    def test_parameters(self):
        return {"portal_type": self.data["@type"], "getReference": self.key}

    @property
    def json_body(self):
        return core.frozendict_to_dict(self.data)

    def complete(self):
        r = self.test_complete()
        result = r.json()["items_total"] >= 1
        return result
