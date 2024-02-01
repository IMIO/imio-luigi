# -*- coding: utf-8 -*-

from imio_luigi import core
from imio_luigi.urban import core as ucore

import json
import logging
import luigi
import os

logger = logging.getLogger("luigi-interface")

class GetJSONFile(core.WalkFS):
    task_namespace = "mouscron"
    extension = ".json"
    path = luigi.Parameter()

    def _fix_ref(self, content):
        ref = content["reference"]
        ref = ref.replace("("," ")
        ref = ref.replace(")"," ")
        ref = ref.replace("  "," ")
        ref = ref.strip()
        content["reference"] = ref
        return content

    def run(self):
        for fpath in self.filepaths:
            with open(fpath, "r") as f:
                content = json.load(f)
                content = self._fix_ref(content)
                yield Transform(key=content["reference"], data=content)


class Transform(luigi.Task):
    task_namespace = "mouscron"
    key = luigi.Parameter()
    data = luigi.DictParameter()
    
    def output(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)

    def run(self):
        with self.output().open("w") as f:
            data = core.frozendict_to_dict(self.data)
            f.write(json.dumps(data))
        yield WriteToJSON(key=self.key)


class WriteToJSON(core.WriteToJSONTask):
    task_namespace = "mouscron"
    export_filepath = "./result-mouscron"
    key = luigi.Parameter()

    def input(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)
