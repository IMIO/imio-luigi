# -*- coding: utf-8 -*-

from imio_luigi import core
from imio_luigi.urban import core as ucore

import json
import logging
import luigi
import os

logger = logging.getLogger("luigi-interface")

class GetJSONFile(core.WalkFS):
    task_namespace = "arlon"
    extension = ".json"
    path = luigi.Parameter()

    def run(self):
        for fpath in self.filepaths:
            with open(fpath, "r") as f:
                content = json.load(f)
                yield Transform(key=os.path.splitext(os.path.basename(fpath))[0], data=content)


class Transform(luigi.Task):
    task_namespace = "arlon"
    key = luigi.Parameter()
    data = luigi.DictParameter()

    def output(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)

    def run(self):
        with self.output().open("w") as f:
            data = core.frozendict_to_dict(self.data)
            f.write(json.dumps(data))
        yield WriteToJSON(key=self.key)


class MappingStateToTransition(ucore.AddNISData):
    task_namespace = "arlon"
    key = luigi.Parameter()

    def transform_data(self, data):
        if "usage" in data:
            return data
        return super().transform_data(data)

    def input(self):
        return core.InMemoryTarget(f"Transform-{self.key}", mirror_on_stderr=True)


class WriteToJSON(core.WriteToJSONTask):
    task_namespace = "arlon"
    export_filepath = "./result-arlon"
    key = luigi.Parameter()

    def requires(self):
        return MappingStateToTransition(key=self.key)
