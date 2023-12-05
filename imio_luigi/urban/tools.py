# -*- coding: utf-8 -*-


from imio_luigi import core, utils

import json
import logging
import luigi
import os
import re

PARTIES_KEYS = ("(pie)", "(partie)", "partie", "parties", "pie", "PIE", "pies")
PARTIES_REGEXP = "|".join(PARTIES_KEYS).replace("(", "\(").replace(")", "\)")
CADASTRE_REGEXP = f"\d{{1,4}} {{0,1}}\w{{1}} {{0,1}}\d{{0,2}} {{0,1}}(?:{PARTIES_REGEXP}){{0,1}}"

MAPPING_TYPE = {
    "BuildLicence": "buildlicence",
    "CODT_BuildLicence": "codt_buildlicence",
    "Article127": "article127",
    "CODT_Article127": "codt_article127",
    "IntegratedLicence": "integratedlicence",
    "CODT_IntegratedLicence": "codt_integratedlicence",
    "UniqueLicence": "uniquelicence",
    "CODT_UniqueLicence": "codt_uniquelicence",
    "Declaration": "declaration",
    "UrbanCertificateOne": "urbancertificateone",
    "CODT_UrbanCertificateOne": "codt_urbancertificateone",
    "UrbanCertificateTwo": "urbancertificatetwo",
    "CODT_UrbanCertificateTwo": "codt_urbancertificatetwo",
    "PreliminaryNotice": "preliminarynotice",
    "EnvClassOne": "envclassone",
    "EnvClassTwo": "envclasstwo",
    "EnvClassThree": "envclassthree",
    "ParcelOutLicence": "parceloutlicence",
    "CODT_ParcelOutLicence": "codt_parceloutlicence",
    "MiscDemand": "miscdemand",
    "NotaryLetter": "notaryletter",
    "CODT_NotaryLetter": "codt_notaryletter",
    "Inspection": "inspection",
    "Ticket": "ticket",
    "Architect": "architect",
    "Geometrician": "geometrician",
    "Notary": "notarie",
    "Parcelling": "parcelling",
    "Division": "division",
    "ProjectMeeting": "projectmeeting",
    "CommercialLicences": "commerciallicence",
    "CODT_CommercialLicences": "codt_commerciallicence",
    "ExplosivesPossessions": "explosivespossession",
    "PatrimonyCertificate" : "patrimonycertificate"
}


def extract_cadastre(value):
    """Extract cadastre informations from a string"""
    value = value.strip()
    value = value.replace(" ", "")

    def _return_value(value, counter, puissance=False):
        partie = False
        for key in PARTIES_KEYS:
            if key in value:
                value = value.replace(key, "")
                partie = True
                break
        result = {
            "radical": value[0:counter],
            "exposant": value[counter].upper(),
        }
        if partie:
            result["partie"] = True
        if puissance:
            result["puissance"] = value[counter + 1 :]
        return result

    for i in reversed(range(1, 5)):
        if re.match(f"^\d{{{i}}}\w{{1}}({PARTIES_REGEXP}){{0,1}}$", value):
            return _return_value(value, i)
        if re.match(f"^\d{{{i}}}\w{{1}}\d{{1,2}}({PARTIES_REGEXP}){{0,1}}$", value):
            return _return_value(value, i, puissance=True)
    raise ValueError(f"Can not parse value '{value}'")


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
            __import__('pdb').set_trace()
            raise ValueError("Missing type")
        if data[self.type_key] in self.get_list:
            data[self.nis_data_key] = self.possible_value[self.get_value(data)]

        return data


class UrbanEventConfigUidResolver(core.GetFromRESTServiceInMemoryTask):
    type_list = ["UrbanEvent"]
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
        error = None
        r = self.request(MAPPING_TYPE[licence_type], data["urbaneventtypes"])
        if r.status_code != 200:
            error = f"Cannot find {data['urbaneventtypes']} in {MAPPING_TYPE[licence_type]} config"
            return data, error
        data["urbaneventtypes"] = r.json()["UID"]
        data['title'] = r.json()["title"]
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
