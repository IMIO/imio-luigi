# -*- coding: utf-8 -*-


from imio_luigi import core, utils

import json
import logging
import luigi
import os
import re

class AddNISData(core.InMemoryTask):
    nis_list_licence_path = "./config/global/list_ins_licence.json"
    nis_data_key = "usage"
    type_key = "@type"
    possible_value = [
        "for_habitation",
        "not_for_habitation",
        "not_applicable"
    ]

    @property
    def get_list(self):
        return json.load(open(self.nis_list_licence_path, "r"))

    def get_value(self, data):
        return 2

    def transform_data(self, data):
        if self.type_key not in data:
            raise ValueError("Missing type")
        if data[self.type_key] in self.get_list:
            data[self.nis_data_key] = self.possible_value[self.get_value(data)]

        return data