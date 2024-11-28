# -*- coding: utf-8 -*-


from datetime import datetime
from imio_luigi import core, utils
from imio_luigi.urban import core as ucore

import abc
import json
import logging
import luigi
import os
import re


class AddNISData(core.InMemoryTask):
    nis_list_licence_path = "./config/global/list_ins_licence.json"
    nis_data_key = "usage"
    type_key = "@type"
    possible_value = ["for_habitation", "not_for_habitation", "not_applicable"]

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


class AddEvents(core.InMemoryTask):
    @property
    def event_config(self):
        raise NotImplementedError

    def _check(self, data, checks):
        return all([check in data and data[check] for check in checks])

    def _add_date(self, event, data, mapping, config):
        dates = mapping.get("date", [])
        for date in dates:
            date_mapping = config["date_mapping"][date]
            date_value = data[date_mapping]
            event[date] = date_value
        return event

    def _add_decision(self, event, data, mapping, config):
        decision_key = mapping.get("decision", None)
        if decision_key is not None:
            wf_transition = data.get("wf_transitions", None)
            if wf_transition is not None:
                return event
            decision = config["decision_mapping"][wf_transition]
            event[decision_key] = decision
        return event

    def transform_data(self, data):
        if "__children__" not in data:
            data["__children__"] = []
        for config in self.event_config.values():
            if data["@type"] not in config["mapping"]:
                continue
            if not self._check(data, config["check_key"]):
                continue
            mapping = config["mapping"][data["@type"]]
            event_type = mapping["urban_type"]
            content_type = mapping.get("@type", "UrbanEvent")
            event = {
                "@type": content_type,
                "urbaneventtypes": event_type
            }
            event = self._add_date(event, data, mapping, config)
            event = self._add_decision(event, data, mapping, config)

            data["__children__"].append(event)
        return data


class AddUrbanEvent(core.InMemoryTask):
    create_recepisse = True
    create_delivery = True
    override_event_path = luigi.OptionalParameter(default=None)
    basic_event_mapping_path = "./config/global/mapping_basic_event.json"
    ignore_event_missing_decision = True

    def transform_data(self, data):
        if self.create_recepisse:
            data = self._create_recepisse(data)
        if self.create_delivery:
            data = self._create_delivery(data)
        return data

    def get_recepisse_check(self, data):
        """Return boolean check for recepisse"""
        if self.create_recepisse:
            raise NotImplementedError
        return None

    def get_recepisse_date(self, data):
        """Return date for recepisse"""
        if self.create_recepisse:
            raise NotImplementedError
        return None

    def get_delivery_check(self, data):
        """Return boolean check for delivery"""
        if self.create_delivery:
            raise NotImplementedError
        return None

    def get_delivery_date(self, data):
        """Return date for delivery"""
        if self.create_delivery:
            raise NotImplementedError
        return None

    def get_delivery_decision(self, data):
        """Return decision for delivery"""
        if self.create_delivery:
            raise NotImplementedError
        return None

    def add_additonal_data_in_recepisse(self, event, data):
        return event

    def handle_failed_check_recepisse(self, data):
        return data

    def _create_recepisse(self, data):
        """Create recepisse event"""
        if data["@type"] in self._no_recepisse_event:
            return data
        if not self.get_recepisse_check(data):
            data = self.handle_failed_check_recepisse(data)
            return data

        event_subtype, event_type = self._mapping_recepisse_event(data["@type"])
        event = {
            "@type": event_type,
            "urbaneventtypes": event_subtype,
        }
        date = self.get_recepisse_date(data)
        if date:
            event["eventDate"] = date

        event = self.add_additonal_data_in_recepisse(event, data)

        if "__children__" not in data:
            data["__children__"] = []
        data["__children__"].append(event)
        return data

    def add_additonal_data_in_delivery(self, event, data):
        return event

    def handle_failed_check_delivery(self, data):
        return data

    def _create_delivery(self, data):
        if data["@type"] in self._no_delivery_event:
            return data

        if not self.get_delivery_check(data):
            data = self.handle_failed_check_delivery(data)
            return data

        event_subtype, event_type = self._mapping_delivery_event(data["@type"])
        event = {
            "@type": event_type,
            "urbaneventtypes": event_subtype,
        }

        decision = self.get_delivery_decision(data)
        if self.ignore_event_missing_decision and not decision:
            return data
        if decision:
            event["decision"] = decision

        event = self.add_additonal_data_in_delivery(event, data)

        date = self.get_delivery_date(data)
        if date:
            event["decisionDate"] = date
            event["eventDate"] = date

        if "__children__" not in data:
            data["__children__"] = []
        data["__children__"].append(event)
        return data

    def get_mapping_overide_file(self):
        if self.override_event_path is None:
            return None
        return json.load(open(self.override_event_path, "r"))

    def get_value_from_override(self, event_type, lic_type):
        overide_mapping = self.get_mapping_overide_file()
        if overide_mapping is None:
            return None
        overide_mapping_event_specific = overide_mapping.get(event_type, None)
        if overide_mapping_event_specific is None:
            return None
        if lic_type not in overide_mapping_event_specific:
            return None
        return tuple(overide_mapping_event_specific[lic_type])

    @property
    def get_basic_event_mapping(self):
        with open(self.basic_event_mapping_path, "r") as f:
            data = json.load(f)
        return data

    def _mapping_recepisse_event(self, type):
        overide_mapping_value = self.get_value_from_override("recepisse", type)
        if overide_mapping_value is not None:
            return overide_mapping_value

        data = self.get_basic_event_mapping["recepisse"]
        return data[type]

    def _mapping_delivery_event(self, type):
        overide_mapping_value = self.get_value_from_override("delivery", type)
        if overide_mapping_value is not None:
            return overide_mapping_value

        data = self.get_basic_event_mapping["delivery"]
        return data[type]

    @property
    def _no_delivery_event(self):
        return ["CODT_NotaryLetter", "ProjectMeeting"]

    @property
    def _no_recepisse_event(self):
        return ["ProjectMeeting"]


class AddAllOtherEvents(core.InMemoryTask):
    use_generic_event = False
    # Use generic event in case of event not found urban
    generic_event_name = ("generic_event", "UrbanEvent")

    @property
    @abc.abstractmethod
    def mapping_filepath(self):
        return None

    @property
    def get_mapping_event(self):
        return json.load(open(self.mapping_filepath, "r"))

    def continue_loop(self, event, data):
        return False

    def transform_data(self, data):
        event_iter = self.get_event_iter(data)
        if event_iter is None:
            return data
        for event in event_iter:
            continue_status = self.continue_loop(event, data)
            if continue_status:
                continue
            param = self.make_paramter(event, data)
            if self.use_generic_event:
                param["title"] = self.get_title(event)
            data = self.create_event(data, **param)
        return data

    def make_paramter(self, event, data):
        event_subtype, event_type = self._handle_get_urbaneventtypes(event, data)
        params = {
            "@type": event_type,
            "urbaneventtypes": event_subtype,
        }
        date = self.get_date(event)
        if date is not None:
            params["eventDate"] = date
        return params

    def _handle_get_urbaneventtypes(self, event, data):
        event_subtype, event_type = self.get_urbaneventtypes(event, data)
        if self.use_generic_event and event_subtype is None:
            return self.generic_event_name
        return event_subtype, event_type

    @abc.abstractmethod
    def get_event_iter(self, data):
        return None

    @abc.abstractmethod
    def get_urbaneventtypes(self, event, data):
        return None, None

    @abc.abstractmethod
    def get_date(self, event):
        return None
    
    def get_title(self, event):
        if self.use_generic_event:
            return NotImplementedError
        return None

    def create_event(self, data, **kwargs):
        if "__children__" not in data:
            data["__children__"] = []
        data["__children__"].append(kwargs)
        return data


class AddUrbanOpinion(AddAllOtherEvents):
    generic_event_name = "generic_opinion"
    child_type = "UrbanEventOpinionRequest"
    generic_opinion_type = "externalDecision"
    mapping_filepath = luigi.Parameter()

    def make_paramter(self, event, data):
        event_subtype, event_type, opinion_type = self._handle_get_urbaneventtypes(event, data)
        params =  {
            "@type": event_type,
            "urbaneventtypes": event_subtype
        }
        date = self.get_date(event)
        if date is not None:
            params["eventDate"] = date
        opinion_type_out = self.get_opinion(event, opinion_type)
        if opinion_type_out is not None:
            params[opinion_type] = opinion_type_out
        return params

    def get_urbaneventtypes(self, event, data):
        return None

    def _handle_get_urbaneventtypes(self, event, data):
        urbaneventtypes = self.get_urbaneventtypes(event, data)
        event_subtype, event_type = urbaneventtypes["type"]
        opinion_type = urbaneventtypes["opinion"]
        if self.use_generic_event and event_subtype is None:
            return self.generic_event_name, self.child_type, self.generic_opinion_type
        return event_subtype, event_type, opinion_type

    @abc.abstractmethod
    def get_opinion(self, event, opinion_type):
        return None

    def create_event(self, data, **kwargs):
        data = super().create_event(data, **kwargs)
        if "solicitOpinionsTo" not in data:
            data["solicitOpinionsTo"] = []
        data["solicitOpinionsTo"].append(kwargs["urbaneventtypes"])
        return data


class CreateApplicant(core.CreateSubElementsFromSubElementsInMemoryTask):
    subelements_source_key = "applicants"
    subelements_destination_key = "__children__"

    @property
    @abc.abstractmethod
    def mapping_keys(self):
        return None

    @property
    @abc.abstractmethod
    def subelement_base(self):
        return None

    def apply_subelement_base_type(self, data):
        self.subelement_base["@type"] = ucore.config[data["@type"]]["contact_type"]

    def transform_data(self, data):
        self.apply_subelement_base_type(data)
        data = super().transform_data(data)
        return data


class AddValuesInDescription(core.InMemoryTask):
    title = ""
    list_style = "ul"
    date_format = "%d/%m/%Y"
    keys_date = []

    @abc.abstractmethod
    def get_values(self, data):
        return None

    @abc.abstractmethod
    def handle_value(self, value, data):
        return data

    def format_date(self, key, value):
        if key in self.keys_date:
            date = datetime.fromisoformat(value)
            value = date.strftime(self.date_format)
        return value

    def transform_value(self, values):
        result = []
        for value in values:
            key = value["key"]
            value = value["value"]
            value = self.format_date(key, value)
            result.append(
                {
                    "key": key,
                    "value": value
                }
            )
        return result

    def transform_data(self, data):
        values = self.get_values(data)

        if values is None:
            return data

        values = self.transform_value(values)

        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }

        if len(values) > 0:
            data["description"]["data"] += f"<H3>{self.title}</H3>\n"
        if len(values) > 1:
            data["description"]["data"] += f"<{self.list_style}>"

        for value in values:
            if len(values) > 1:
                data["description"]["data"] += "<li>"
            data = self.handle_value(value, data)
            if len(values) > 1:
                data["description"]["data"] += "</li>\n"

        if len(values) > 1:
            data["description"]["data"] += f"</{self.list_style}>\n"

        return data
