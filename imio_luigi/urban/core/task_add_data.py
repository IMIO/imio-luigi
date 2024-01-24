# -*- coding: utf-8 -*-


from imio_luigi import core, utils

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


class AddUrbanEvent(core.InMemoryTask):
    create_recepisse = True
    create_delivery = True
    
    def transform_data(self, data):
        if self.create_recepisse: 
            data = self._create_recepisse(data)
        if self.create_delivery: 
            data = self._create_delivery(data)
        return data

    @abc.abstractmethod
    def get_recepisse_check(self, data):
        """Return boolean check for recepisse"""
        return None
    
    @abc.abstractmethod
    def get_recepisse_date(self, data):
        """Return date for recepisse"""
        return None
    
    @abc.abstractmethod
    def get_delivery_check(self, data):
        """Return boolean check for delivery"""
        return None

    @abc.abstractmethod
    def get_delivery_date(self, data):
        """Return date for delivery"""
        return None

    @abc.abstractmethod
    def get_delivery_decision(self, data):
        """Return decision for delivery"""
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
    
    def _mapping_recepisse_event(self, type):
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
            "CODT_NotaryLetter": ("depot-de-la-demande-codt", "UrbanEvent"),
            "Division": ("depot-de-la-demande", "UrbanEvent"),
            "CODT_CommercialLicence": ("depot-demande", "UrbanEvent"),
            "ExplosivesPossession": ("reception-de-la-demande", "UrbanEvent"),
            "Ticket": ("depot-de-la-demande-codt", "UrbanEvent"),
        }
        return data[type]
    
    def _mapping_delivery_event(self, type):
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
            "CODT_IntegratedLicence": ("delivrance-du-permis-octroi-ou-refus-codt", "UrbanEvent"),
            "ParcelOutLicence": ("delivrance-du-permis-octroi-ou-refus", "UrbanEvent"),
            "CODT_ParcelOutLicence": ("delivrance-du-permis-octroi-ou-refus-codt", "UrbanEvent"),
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
            "CODT_NotaryLetter": ("notaryletter-codt", "UrbanEvent"),
            "Division": ("decision-octroi-refus", "UrbanEvent"),
            "CODT_CommercialLicence": ("delivrance-du-permis-octroi-ou-refus-codt", " UrbanEvent"),
            "ExplosivesPossession": {"decision", "UrbanEvent"},
            "Ticket": ("decision", "UrbanEvent"),
        }
        return data[type]
