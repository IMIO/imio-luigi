# -*- coding: utf-8 -*-

from imio_luigi import core, utils
from imio_luigi.urban import core as ucore
from imio_luigi.urban.address import find_address_match

import abc
import json
import logging
import luigi
import os
import re


class TransformWorkLocation(core.GetFromRESTServiceInMemoryTask):
    search_match = True
    seach_disable = True

    @property
    def request_url(self):
        return f"{self.url}/@address"

    def on_failure(self, data, errors):
        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }
        for error in errors:
            data["description"]["data"] += f"<p>{error}</p>\r\n"
        return data

    def _generate_term(self, worklocation, data):
        """Generate term for street to be search, return tuple of term and error"""
        return None, None

    def _generate_street_code(self, worklocation, data):
        """Generate street code for street to be search, return tuple of street code and error"""
        return None, None

    def transform_data(self, data):
        new_work_locations = []
        errors = []
        for worklocation in data["workLocations"]:
            params = {"match": self.search_match, "include_disable": self.seach_disable}

            term, error = self._generate_term(worklocation, data)
            if error:
                errors.append(error)
                continue
            if term:
                params["term"] = term

            street_code, error = self._generate_street_code(worklocation, data)

            if error:
                errors.append(error)
                continue
            if street_code:
                params["street_code"] = street_code

            if term is None and street_code is None:
                raise NotImplementedError(
                    "At least one of '_generate_term' or '_generate_street_code' must be impleted"
                )

            r = self.request(parameters=params)
            if r.status_code != 200:
                errors.append(f"Response code is '{r.status_code}', expected 200")
                continue
            result = r.json()
            if result["items_total"] == 0:
                errors.append(f"Aucun résultat pour l'adresse: '{params['term']}'")
                continue
            elif result["items_total"] > 1:
                match = find_address_match(result["items"], worklocation["street"])
                if not match:
                    errors.append(
                        f"Plusieurs résultats pour l'adresse: '{params['term']}'"
                    )
                    continue
            else:
                match = result["items"][0]
            new_work_locations.append(
                {
                    "street": match["uid"],
                    "number": worklocation.get("number", ""),
                }
            )
        data["workLocations"] = new_work_locations
        return data, errors


class TransformCadastre(core.GetFromRESTServiceInMemoryTask):
    browse_old_parcels = True

    @property
    def request_url(self):
        return f"{self.url}/@parcel"

    def on_failure(self, data, errors):
        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }
        for error in errors:
            # cleanup
            error = error.replace(", 'browse_old_parcels': True", "")
            error = error.replace("'", "")
            data["description"]["data"] += f"<p>{error}</p>\r\n"
        return data

    @abc.abstractmethod
    def _generate_cadastre_dict(self, cadastre, data):
        """Generate cadastre dict, return tuple of cadastre and error"""
        return None, None

    def _check_for_duplicate_cadastre(self, cadastre, children):
        child_cadastre = [
            child["id"] for child in children if child.get("@type", None) == "Parcel"
        ]
        return cadastre["id"] not in child_cadastre

    def transform_data(self, data):
        errors = []
        if "cadastre" not in data:
            return data, errors
        for cadastre in data["cadastre"]:
            params, error = self._generate_cadastre_dict(cadastre, data)
            if error:
                errors.append(error)
                continue
            params["browse_old_parcels"] = self.browse_old_parcels
            r = self.request(parameters=params)
            if r.status_code != 200:
                errors.append(f"Response code is '{r.status_code}', expected 200")
                continue
            result = r.json()
            if result["items_total"] == 0:
                del params["browse_old_parcels"]
                errors.append(f"Aucun résultat pour la parcelle '{params}'")
                continue
            elif result["items_total"] > 1:
                del params["browse_old_parcels"]
                errors.append(f"Plusieurs résultats pour la parcelle '{params}'")
                continue
            if not "__children__" in data:
                data["__children__"] = []
            new_cadastre = result["items"][0]
            new_cadastre["@type"] = "Parcel"
            if "capakey" in new_cadastre:
                new_cadastre["id"] = new_cadastre["capakey"].replace("/", "_")
            if "old" in new_cadastre:
                new_cadastre["outdated"] = new_cadastre["old"]
            else:
                new_cadastre["outdated"] = False
            for key in ("divname", "natures", "locations", "owners", "capakey", "old"):
                if key in new_cadastre:
                    del new_cadastre[key]
            if self._check_for_duplicate_cadastre(new_cadastre, data["__children__"]):
                data["__children__"].append(new_cadastre)
        return data, errors


class TransformContact(core.GetFromRESTServiceInMemoryTask):
    contact_type = ""
    data_key = ""

    @property
    def request_url(self):
        return f"{self.url}/urban/{self.contact_type}/@search"

    def on_failure(self, data, errors):
        if "description" not in data:
            data["description"] = {
                "content-type": "text/html",
                "data": "",
            }
        for error in errors:
            data["description"]["data"] += f"<p>{error}</p>\r\n"
        return data

    @abc.abstractmethod
    def _generate_contact_name(self, data):
        """Generate contact name, return tuple of contact name and error"""
        return None, None

    def transform_data(self, data):
        errors = []
        search, error = self._generate_contact_name(data)
        if error:
            errors.append(error)
            return data, errors
        params = {"SearchableText": f"{search}", "metadata_fields": "UID"}
        r = self.request(parameters=params)
        if r.status_code != 200:
            errors.append(f"Response code is '{r.status_code}', expected 200")
            return data, errors
        result = r.json()
        if result["items_total"] == 0:
            errors.append(f"Aucun résultat pour: '{search}'")
            return data, errors
        elif result["items_total"] > 1:
            errors.append(f"Plusieurs résultats pour: '{search}'")
            return data, errors
        data[self.data_key] = [result["items"][0]["UID"]]
        return data, errors
