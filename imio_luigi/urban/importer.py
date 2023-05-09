# -*- coding: utf-8 -*-

from imio_luigi import core

import base64
import json
import logging
import luigi


logger = logging.getLogger("luigi-interface")


class GetFiles(core.WalkFS):
    task_namespace = "urban"
    extension = ".json"
    path = luigi.Parameter()
    url = luigi.Parameter()
    login = luigi.Parameter()
    password = luigi.Parameter()

    def _get_url(self, data):
        """Construct POST url based on type"""
        mapping = {
            "BuildLicence": "buildlicences",
            "CODT_BuildLicence": "codt_buildlicences",
            "Article127": "article127s",
            "IntegratedLicence": "integratedlicences",
            "CODT_IntegratedLicence": "codt_integratedlicences",
            "UniqueLicence": "uniquelicences",
            "CODT_UniqueLicence": "codt_uniquelicences",
            "Declaration": "declarations",
            "UrbanCertificateOne": "urbancertificateones",
            "CODT_UrbanCertificateOne": "codt_urbancertificateones",
            "UrbanCertificateTwo": "urbancertificatetwos",
            "CODT_UrbanCertificateTwo": "codt_urbancertificatetwos",
            "PreliminaryNotice": "preliminarynotices",
            "EnvClassOne": "envclassones",
            "EnvClassTwo": "envclasstwos",
            "EnvClassThree": "envclassthrees",
            "ParcelOutLicence": "parceloutlicences",
            "CODT_ParcelOutLicence": "codt_parceloutlicences",
            "MiscDemand": "miscdemands",
            "NotaryLetter": "notaryletters",
            "CODT_NotaryLetter": "codt_notaryletters",
            "Inspection": "inspections",
            "Ticket": "tickets",
            "Architect": "architects",
            "Geometrician": "geometricians",
            "Notary": "notaries",
            "Parcelling": "parcellings",
            "Division": "divisions",
        }
        folder = mapping[data["type"]]
        return f"{self.url}/{folder}"

    def run(self):
        for fpath in self.filepaths:
            with open(fpath, "r") as f:
                content = json.load(f)
                yield RESTPost(
                    key=content["reference"],
                    data=content,
                    url=self._get_url(content),
                    login=self.login,
                    password=self.password,
                )


class RESTPost(core.PostRESTTask):
    task_namespace = "urban"
    key = luigi.Parameter()
    data = luigi.DictParameter()
    url = luigi.Parameter()
    login = luigi.Parameter()
    password = luigi.Parameter()

    @property
    def test_url(self):
        return f"{self.url}/@search"

    def run(self):
        result = self.output().request()

    @property
    def test_parameters(self):
        return {"portal_type": self.data["@type"], "getReference": self.key}

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
        return self._add_attachments(core.frozendict_to_dict(self.data))

    def complete(self):
        r = self.test_complete()
        result = r.json()["items_total"] >= 1
        return result
