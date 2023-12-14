# -*- coding: utf-8 -*-


from imio_luigi import core, utils
from imio_luigi.urban import core as ucore

import json
import logging
import luigi
import os
import re

class UrbanTransitionMapping(core.InMemoryTask):
    log_failure = False
    
    def log_failure_output(self):
        fname = self.key.replace("/", "-")
        fpath = (
            f"./failures/{self.task_namespace}-"
            f"{self.__class__.__name__}/{fname}.json"
        )
        return luigi.LocalTarget(fpath)
    
    def on_failure(self, data, errors):
        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }
        for error in errors:
            data["description"]["data"] += f"<p>{error}</p>\r\n"
        return data

    def _handle_exception(self, data, error, output_f):
        """Method called when an exception occured"""
        if not self.log_failure:
            raise error
        data = self.on_failure(data, [str(error)])
        json.dump(data, output_f)
        with self.log_failure_output().open("w") as f:
            error = {
                "error": str(error),
                "data": data,
            }
            f.write(json.dumps(error))

    def transform_data(self, data):
        config = ucore.config
        workflows = ucore.workflows

        lic_type = data.get('@type', None)
        if lic_type is None:
            raise RuntimeError("Missing type")
        if lic_type not in config:
            raise RuntimeError("Wrong type")

        workflow = config[lic_type]["workflow"]
        target_state = data.get("wf_transitions", None)
        if not target_state or len(target_state) == 0:
            data["wf_transitions"] = []
            return data

        target_state = target_state[0]
        if target_state not in workflows[workflow]["transition"]:
            if target_state in workflows[workflow]["mapping"]:
                target_state = workflows[workflow]["mapping"][target_state]
            else:
                data["wf_transitions"] = []
                return data
        
        data["wf_transitions"] = workflows[workflow]["transition"][target_state]
        return data
    
    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                try :
                    json.dump(self.transform_data(data), output_f)
                except Exception as e:
                    self._handle_exception(data, e, output_f)
