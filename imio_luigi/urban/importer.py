# -*- coding: utf-8 -*-

from imio_luigi import core

import base64
import json
import logging
import luigi


logger = logging.getLogger("luigi-interface")

DEFAULT_CONFIG = {
    "key":  "reference",
    "search_on": "getReference"
}

CONTACT_CONFIG = {
    "key":  "id",
    "search_on": "id"
}

class GetFiles(core.WalkFS):
    task_namespace = "urban"
    extension = ".json"
    path = luigi.Parameter()
    url = luigi.Parameter()
    login = luigi.Parameter()
    password = luigi.Parameter()
    mapping = {
        "BuildLicence": {
            "folder": "buildlicences",
            "config": DEFAULT_CONFIG
        },
        "CODT_BuildLicence": {
            "folder": "codt_buildlicences",
            "config": DEFAULT_CONFIG
        },
        "Article127": {
            "folder": "article127s",
            "config": DEFAULT_CONFIG
        },
        "CODT_Article127": {
            "folder": "codt_article127s",
            "config": DEFAULT_CONFIG
        },
        "IntegratedLicence": {
            "folder": "integratedlicences",
            "config": DEFAULT_CONFIG
        },
        "CODT_IntegratedLicence": {
            "folder": "codt_integratedlicences",
            "config": DEFAULT_CONFIG
        },
        "UniqueLicence": {
            "folder": "uniquelicences",
            "config": DEFAULT_CONFIG
        },
        "CODT_UniqueLicence": {
            "folder": "codt_uniquelicences",
            "config": DEFAULT_CONFIG
        },
        "Declaration": {
            "folder": "declarations",
            "config": DEFAULT_CONFIG
        },
        "UrbanCertificateOne": {
            "folder": "urbancertificateones",
            "config": DEFAULT_CONFIG
        },
        "CODT_UrbanCertificateOne": {
            "folder": "codt_urbancertificateones",
            "config": DEFAULT_CONFIG
        },
        "UrbanCertificateTwo": {
            "folder": "urbancertificatetwos",
            "config": DEFAULT_CONFIG
        },
        "CODT_UrbanCertificateTwo": {
            "folder": "codt_urbancertificatetwos",
            "config": DEFAULT_CONFIG
        },
        "PreliminaryNotice": {
            "folder": "preliminarynotices",
            "config": DEFAULT_CONFIG
        },
        "EnvClassOne": {
            "folder": "envclassones",
            "config": DEFAULT_CONFIG
        },
        "EnvClassTwo": {
            "folder": "envclasstwos",
            "config": DEFAULT_CONFIG
        },
        "EnvClassThree": {
            "folder": "envclassthrees",
            "config": DEFAULT_CONFIG
        },
        "ParcelOutLicence": {
            "folder": "parceloutlicences",
            "config": DEFAULT_CONFIG
        },
        "CODT_ParcelOutLicence": {
            "folder": "codt_parceloutlicences",
            "config": DEFAULT_CONFIG
        },
        "MiscDemand": {
            "folder": "miscdemands",
            "config": DEFAULT_CONFIG
        },
        "NotaryLetter": {
            "folder": "notaryletters",
            "config": DEFAULT_CONFIG
        },
        "CODT_NotaryLetter": {
            "folder": "codt_notaryletters",
            "config": DEFAULT_CONFIG
        },
        "Inspection": {
            "folder": "inspections",
            "config": DEFAULT_CONFIG
        },
        "Ticket": {
            "folder": "tickets",
            "config": DEFAULT_CONFIG
        },
        "Architect": {
            "folder": "architects",
            "config": CONTACT_CONFIG
        },
        "Geometrician": {
            "folder": "geometricians",
            "config": CONTACT_CONFIG
        },
        "Notary": {
            "folder": "notaries",
            "config": CONTACT_CONFIG
        },
        "Parcelling": {
            "folder": "parcellings",
            "config": DEFAULT_CONFIG
        },
        "Division": {
            "folder": "divisions",
            "config": DEFAULT_CONFIG
        },
        "ProjectMeeting": {
            "folder": "projectmeeting",
            "config": DEFAULT_CONFIG
        },
        "CommercialLicences": {
            "folder": "commerciallicences",
            "config": DEFAULT_CONFIG
        },
        "CODT_CommercialLicences": {
            "folder": "codt_commerciallicences",
            "config": DEFAULT_CONFIG
        },
        "ExplosivesPossessions": {
            "folder":"explosivespossessions",
            "config": DEFAULT_CONFIG
        }
    }
    
    def _get_url(self, data):
        """Construct POST url based on type"""
        folder = self.mapping[data["@type"]]["folder"]
        return f"{self.url}/{folder}"

    def run(self):
        for fpath in self.filepaths:
            with open(fpath, "r") as f:
                content = json.load(f)
                yield RESTPost(
                    key=content[self.mapping[content["@type"]]["config"]["key"]],
                    data=content,
                    url=self._get_url(content),
                    login=self.login,
                    password=self.password,
                    search_on=self.mapping[content["@type"]]["config"]["search_on"]
                )


class RESTPost(core.PostRESTTask):
    task_namespace = "urban"
    key = luigi.Parameter()
    data = luigi.DictParameter()
    url = luigi.Parameter()
    login = luigi.Parameter()
    password = luigi.Parameter()
    search_on = luigi.Parameter()

    @property
    def test_url(self):
        return f"{self.url}/@search"

    def run(self):
        result = self.output().request()

    def _fix_character(self, term, character):
        if character in term:
            joiner = '"{}"'.format(character)
            term = joiner.join(term.split(character))
        return term

    def _fix_key(self, key):
        key = self._fix_character(key, "(")
        key = self._fix_character(key, ")")
        return key

    @property
    def test_parameters(self):
        return {"portal_type": self.data["@type"], self.search_on: self._fix_key(self.key)}

    def _add_attachments(self, data):
        if "__children__" not in data:
            return data
        for child in data["__children__"]:
            if "file" not in child:
                continue
            with open(child["file"]["data"], "rb") as f:
                content = f.read()
                child["file"]["data"] = base64.b64encode(content).decode("utf8")
        return data

    @property
    def json_body(self):
        json_body = self._add_attachments(core.frozendict_to_dict(self.data))
        json_body["disable_check_ref_format"] = True
        return json_body

    def complete(self):
        r = self.test_complete()
        result = r.json()["items_total"] >= 1
        return result
