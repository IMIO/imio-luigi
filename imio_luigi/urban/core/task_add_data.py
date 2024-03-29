# -*- coding: utf-8 -*-


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


class AddUrbanEvent(core.InMemoryTask):
    create_recepisse = True
    create_delivery = True
    override_event_path = luigi.OptionalParameter(default=None)

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

    def _create_recepisse(self, data):
        """Create recepisse event"""
        if not self.get_recepisse_check(data):
            return data

        event_subtype, event_type = self._mapping_recepisse_event(data["@type"])
        event = {
            "@type": event_type,
            "urbaneventtypes": event_subtype,
        }
        date = self.get_recepisse_date(data)
        if date:
            event["eventDate"] = date

        if "__children__" not in data:
            data["__children__"] = []
        data["__children__"].append(event)
        return data

    def _create_delivery(self, data):
        if data["@type"] in self._no_delivery_event:
            return data

        if not self.get_delivery_check(data):
            return data

        decision = self.get_delivery_decision(data)
        if not decision:
            return data

        event_subtype, event_type = self._mapping_delivery_event(data["@type"])
        event = {
            "@type": event_type,
            "decision": decision,
            "urbaneventtypes": event_subtype,
        }

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

    def _mapping_recepisse_event(self, type):
        overide_mapping_value = self.get_value_from_override("recepisse", type)
        if overide_mapping_value is not None:
            return overide_mapping_value

        data = {
            "BuildLicence": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_BuildLicence": ("depot-de-la-demande-codt", "UrbanEvent"),
            "Article127": ("depot-de-la-demande", "UrbanEvent"),
            "IntegratedLicence": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_IntegratedLicence": ("depot-de-la-demande-codt", "UrbanEvent"),
            "UniqueLicence": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_UniqueLicence": ("depot-de-la-demande", "UrbanEvent"),
            "Declaration": ("depot-de-la-demande", "UrbanEvent"),
            "UrbanCertificateOne": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_UrbanCertificateOne": ("depot-de-la-demande-codt", "UrbanEvent"),
            "UrbanCertificateTwo": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_UrbanCertificateTwo": ("depot-demande", "UrbanEvent"),
            "PreliminaryNotice": ("depot-de-la-demande", "UrbanEvent"),
            "EnvClassOne": ("depot-de-la-demande", "UrbanEvent"),
            "EnvClassTwo": ("depot-de-la-demande", "UrbanEvent"),
            "EnvClassThree": ("depot-de-la-demande", "UrbanEvent"),
            "ParcelOutLicence": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_ParcelOutLicence": ("depot-de-la-demande-codt", "UrbanEvent"),
            "MiscDemand": ("depot-de-la-demande", "UrbanEvent"),
            "NotaryLetter": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_NotaryLetter": ("notaryletter-codt", "UrbanEvent"),
            "Division": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_CommercialLicence": ("depot-demande", "UrbanEvent"),
            "ExplosivesPossession": ("reception-de-la-demande", "UrbanEvent"),
            "Ticket": ("depot-de-la-demande-codt", "UrbanEvent"),
        }
        return data[type]

    def _mapping_delivery_event(self, type):
        overide_mapping_value = self.get_value_from_override("delivery", type)
        if overide_mapping_value is not None:
            return overide_mapping_value

        data = {
            "BuildLicence": ("delivrance-du-permis-octroi-ou-refus", "UrbanEvent"),
            "CODT_BuildLicence": (
                "delivrance-du-permis-octroi-ou-refus-codt",
                "UrbanEvent",
            ),
            "CODT_UrbanCertificateOne": (
                "delivrance-du-permis-octroi-ou-refus-codt",
                "UrbanEvent",
            ),
            "CODT_UrbanCertificateTwo": (
                "delivrance-du-permis-octroi-ou-refus-codt",
                "UrbanEvent",
            ),
            "Article127": ("delivrance-du-permis-octroi-ou-refus", "UrbanEvent"),
            "IntegratedLicence": ("delivrance-du-permis-octroi-ou-refus", "UrbanEvent"),
            "CODT_IntegratedLicence": (
                "delivrance-du-permis-octroi-ou-refus-codt",
                "UrbanEvent",
            ),
            "ParcelOutLicence": ("delivrance-du-permis-octroi-ou-refus", "UrbanEvent"),
            "CODT_ParcelOutLicence": (
                "delivrance-du-permis-octroi-ou-refus-codt",
                "UrbanEvent",
            ),
            "Declaration": ("deliberation-college", "UrbanEvent"),
            "UrbanCertificateOne": ("octroi-cu1", "UrbanEvent"),
            "UrbanCertificateTwo": ("octroi-cu2", "UrbanEvent"),
            "UniqueLicence": ("delivrance-du-permis-octroi-ou-refus", "UrbanEvent"),
            "CODT_UniqueLicence": ("delivrance-permis", "UrbanEvent"),
            "MiscDemand": ("deliberation-college", "UrbanEvent"),
            "EnvClassOne": ("decision", "UrbanEvent"),
            "EnvClassTwo": ("decision", "UrbanEvent"),
            "EnvClassThree": ("passage-college", "UrbanEvent"),
            "PreliminaryNotice": ("passage-college", "UrbanEvent"),
            "NotaryLetter": ("octroi-lettre-notaire", " UrbanEvent"),
            "Division": ("decision-octroi-refus", "UrbanEvent"),
            "CODT_CommercialLicence": (
                "delivrance-du-permis-octroi-ou-refus-codt",
                " UrbanEvent",
            ),
            "ExplosivesPossession": {"decision", "UrbanEvent"},
            "Ticket": ("decision", "UrbanEvent"),
        }
        return data[type]

    @property
    def _no_delivery_event(self):
        return ["CODT_NotaryLetter"]


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
