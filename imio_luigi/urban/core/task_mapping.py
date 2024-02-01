# -*- coding: utf-8 -*-

from imio_luigi import core, utils
from imio_luigi.urban import core as ucore
from datetime import datetime

import abc
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

        lic_type = data.get("@type", None)
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
                try:
                    json.dump(self.transform_data(data), output_f)
                except Exception as e:
                    self._handle_exception(data, e, output_f)


class UrbanTypeMapping(core.MappingValueWithFileInMemoryTask):
    log_failure = True
    codt_trigger = [
        "UrbanCertificateOne",
        "UrbanCertificateTwo",
        "Article127",
        "BuildLicence",
        "ParcelOutLicence",
        "NotaryLetter",
        "UniqueLicence",
        "IntegratedLicence",
    ]
    codt_start_date = datetime(2017, 6, 1)
    codt_start_year = 2017
    mapping_key = "@type"
    check_codt_date = False

    def on_failure(self, data, errors):
        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }
        for error in errors:
            data["description"]["data"] += f"<p>{error}</p>\r\n"
        return data

    def log_failure_output(self):
        fname = self.key.replace("/", "-")
        fpath = (
            f"./failures/{self.task_namespace}-"
            f"{self.__class__.__name__}/{fname}.json"
        )
        return luigi.LocalTarget(fpath)

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

    def get_date(self, data):
        return None

    def get_year(self, data):
        return None

    def ensure_type_string(self, data):
        data[self.mapping_key] = str(data[self.mapping_key])
        return data

    def _cwatup_codt(self, data):
        if self.mapping_key not in data:
            raise (f"Manque la clé {self.mapping_key}")
        if data[self.mapping_key] not in ucore.config:
            raise (
                f"le type : {data[self.mapping_key]} n'est pas un type présent dans Urban"
            )
        if data[self.mapping_key] not in self.codt_trigger:
            return data

        year = None
        date = self.get_date(data)
        if not date:
            year = self.get_year(data)

        if not date or year == self.codt_start_year:
            raise KeyError("Manque une date pour déterminer si c'est un permis CODT")

        date = datetime.fromisoformat(date)

        if date > self.codt_start_date:
            data[self.mapping_key] = f"CODT_{data[self.mapping_key]}"

        if year and year > self.codt_start_year:
            data[self.mapping_key] = f"CODT_{data[self.mapping_key]}"

        return data

    def transform_data(self, data):
        data = self.ensure_type_string(data)
        data = super().transform_data(data)
        if self.check_codt_date:
            data = self._cwatup_codt(data)
        return data

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                try:
                    json.dump(self.transform_data(data), output_f)
                except Exception as e:
                    self._handle_exception(data, e, output_f)
