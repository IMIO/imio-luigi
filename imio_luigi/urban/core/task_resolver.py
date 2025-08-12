# -*- coding: utf-8 -*-


from imio_luigi import core, utils
from imio_luigi.urban import core as ucore

import json
import logging
import luigi
import os
import re


class UrbanEventConfigUidResolver(core.GetFromRESTServiceInMemoryTask):
    type_list = ["UrbanEvent", "UrbanEventOpinionRequest", "UrbanEventInquiry", "UrbanEventAnnouncement"]
    log_failure = True

    def on_failure(self, data, errors):
        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }
        for error in errors:
            data["description"]["data"] += f"<p>{error}</p>\r\n"
        return data

    def request(self, licence_type, event_config, parameters=None):
        """Perform the REST request"""
        if parameters is None:
            parameters = self.parameters
        auth = None
        if self.login and self.password:
            auth = (self.login, self.password)
        return self._request(
            f"{self.url}/portal_urban/{licence_type}/eventconfigs/{event_config}",
            headers={"Accept": self.accept},
            auth=auth,
            params=parameters,
        )

    def _transform_urban_event_type(self, data, licence_type):
        mapping_type = ucore.config
        error = None
        r = self.request(
            mapping_type[licence_type]["config_folder"], data["urbaneventtypes"]
        )
        if r.status_code != 200:
            error = f"Cannot find {data['urbaneventtypes']} in {mapping_type[licence_type]['config_folder']} config"
            return data, error
        data["urbaneventtypes"] = r.json()["UID"]
        data["title"] = r.json()["title"]
        return data, error

    def transform_data(self, data):
        children = data.get("__children__", None)
        errors = []

        if not children:
            return data, errors

        new_children = []
        for child in children:
            if child["@type"] in self.type_list:
                child, error = self._transform_urban_event_type(child, data["@type"])
                if error:
                    errors.append(error)
            new_children.append(child)

        data["__children__"] = new_children

        return data, errors
