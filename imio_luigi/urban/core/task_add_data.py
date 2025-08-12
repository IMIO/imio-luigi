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
    """
    Add mandatory NIS value to data for some licence type
    """
    nis_list_licence_path = "./config/global/list_ins_licence.json"
    nis_data_key = "usage"
    type_key = "@type"
    possible_value = ["for_habitation", "not_for_habitation", "not_applicable"]
    log_failure = True

    @property
    def get_list(self):
        return json.load(open(self.nis_list_licence_path, "r"))

    def get_value(self, data):
        """
        Method that can be overridden to return value from data

        :param data: licence data
        :type data: dict
        :return: the value expected from 0 to 2, 0 = for private use, 1 = for other than private use, 2 = n/a
        :rtype: int
        """
        return 2

    def transform_data(self, data):
        if self.type_key not in data:
            raise ValueError("Missing type")
        if data[self.type_key] in self.get_list:
            data[self.nis_data_key] = self.possible_value[self.get_value(data)]
        return data

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

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                try:
                    json.dump(self.transform_data(data), output_f)
                except Exception as e:
                    self._handle_exception(data, e, output_f)


class AddEvents(core.InMemoryTask):
    """
    Task to add urban event to data
    """

    @property
    def event_config(self):
        """
        _summary_

        :raises NotImplementedError: _description_
        """
        raise NotImplementedError

    def check(self, data, config):
        if "check_key" in config or "check_all_key" in config:
            check_key = config.get("check_all_key", config["check_key"])
            return not self._check_all(data, check_key, parents=config.get("parents_keys", None))
        if "check_any_key" in config:
            return not self._check_any(data, config["check_any_key"], parents=config.get("parents_keys", None))
        return False

    def _check_all(self, data, checks, parents):
        return all([utils.get_value_from_path_with_parents(data, check, parents) for check in checks])

    def _check_any(self, data, checks, parents):
        return any([utils.get_value_from_path_with_parents(data, check, parents) for check in checks])

    def get_check_key_list(self, config):
        check_key = "check_key"
        if "check_all_key" in config:
            check_key = "check_all_key"
        elif "check_any_key" in config:
            check_key = "check_any_key"
        return config[check_key]

    def _add_date(self, event, data, mapping, config):
        dates = mapping.get("date", [])
        for date in dates:
            date_mapping = config["date_mapping"][date]
            if "*" in date_mapping:
                date_mapping.remove("*")
                date_mapping += self.get_check_key_list(config)
            if isinstance(date_mapping, str):
                date_mapping = [date_mapping]
            for date_map in date_mapping:
                date_value = utils.get_value_from_path_with_parents(data, date_map, config.get("parents_keys", None))
                if date_value is not None:
                    continue
            if date_value is None:
                continue
            event[date] = date_value
        return event

    def _add_decision(self, event, data, mapping, config):
        decision_key = mapping.get("decision", None)
        if decision_key is None:
            return event
        decision_mapping = config.get("decision_mapping", None)
        if decision_mapping is None:
            raise KeyError("Missing decision_mapping")

        decision_value = utils.get_value_from_path_with_parents(data, decision_mapping.get(decision_key, ""), config.get("parents_keys", None))
        decision_value = config["decision_value_mapping"].get(decision_key,{}).get(decision_value, None)
        if decision_value is not None:
            event[decision_key] = decision_value
        return event

    def transform_data(self, data):
        if "@type" not in data:
            return data
        if "__children__" not in data:
            data["__children__"] = []
        for config in self.event_config.values():
            if data["@type"] not in config["mapping"]:
                continue
            if self.check(data, config):
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
    override_event = luigi.OptionalParameter(default=None)
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
        if "@type" not in data:
            return data
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
        if "@type" not in data:
            return data
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
        if self.override_event_path is None and self.override_event is None:
            return None
        if self.override_event :
            return self.override_event
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
            output = {}
            for key, dict_value in value.items():
                dict_value = self.format_date(key, dict_value)
                output[key] = dict_value
            result.append(output)
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
