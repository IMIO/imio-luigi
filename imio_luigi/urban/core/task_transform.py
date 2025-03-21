# -*- coding: utf-8 -*-

from imio_luigi import core, utils
from imio_luigi.urban import core as ucore
from imio_luigi.urban.address import find_address_similarity

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

    def _handle_failed_street_code(self, worklocation, data):
        """handle in case we failed to retrieve worklocation with street code"""
        if self._generate_street_code(worklocation, data)[0]:
            raise NotImplementedError
        return None, None

    def generate_street_item(self, street_uid, number):
        """
        handle street item creation

        :param street_uid: uid of the street
        :type street_uid: string
        :param number: street number
        :type number: string
        :return: list of street items
        :rtype: list of dict
        """
        return  [{
            "street": street_uid,
            "number": number,
        }]

    def transform_data(self, data):
        new_work_locations = []
        errors = []
        for worklocation in data["workLocations"]:
            params = {"match": self.search_match, "include_disable": self.seach_disable}

            self.term, error = self._generate_term(worklocation, data)
            if error:
                errors.append(error)
                continue
            if self.term:
                self.error_print = "term"
                params["term"] = self.term

            self.street_code, error = self._generate_street_code(worklocation, data)

            if error:
                errors.append(error)
                continue
            if self.street_code:
                self.error_print = "street_code"
                params["street_code"] = self.street_code

            if self.term is None and self.street_code is None:
                raise NotImplementedError(
                    "At least one of '_generate_term' or '_generate_street_code' must be impleted"
                )

            r = self.request(parameters=params)
            if r.status_code != 200:
                errors.append(f"Response code is '{r.status_code}', expected 200")
                continue
            result = r.json()
            if result["items_total"] == 0 and self.street_code and self._handle_failed_street_code(worklocation, data) is not None:
                result, error = self._handle_failed_street_code(worklocation, data)
                if error:
                    errors.append(error)
                    continue
            if result["items_total"] == 0:
                error = "Aucun résultat"
                if self.error_print == "street_code":
                    error += f" pour le code de rue: '{self.street_code}'"
                elif self.error_print == "term":
                    error += f" pour l'adresse: '{self.term}'"
                errors.append(error)
                continue
            elif result["items_total"] > 1:
                match, similarity_error = find_address_similarity(result["items"], self.term)
                if not match:
                    error = "Plusieurs résultats"
                    if self.error_print == "street_code":
                        error += f" pour le code de rue: '{self.street_code}'"
                    elif self.error_print == "term":
                        error += f" pour l'adresse: '{self.term}'"
                    errors.append(error)
                    continue
                if similarity_error:
                    errors.append(similarity_error)
            else:
                match = result["items"][0]
            new_work_locations += self.generate_street_item(match["uid"], worklocation.get("number", ""))
        data["workLocations"] = new_work_locations
        return data, errors


class TransformWorkLocationMultiParams(core.GetFromRESTServiceInMemoryTask):
    search_match = True
    seach_disable = True

    @property
    def report_path(self):
        return None

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

    def _generate_params(self, worklocation, data):
        """
            Generate list of param for street to be search, return tuple of list and error
            list schema : 
            [
                {
                    key : "term" | "street_code",
                    value : string | street_code
                    handle_error : None | func(params, worklocation, data) -> data, action
                }
            ]
        """
        return None, None

    def _output_report(self, result, expectation):
        if self.report_path is None:
            return
        with open(self.report_path, "a") as f:
            f.write(f"{self.key} : result : {result}, expectation : {expectation}\n")

    def request_params(self, params, params_obj):
        r = self.request(parameters=params)
        if r.status_code != 200:
            return None, f"Response code is '{r.status_code}', expected 200"
        result = r.json()
        if result["items_total"] == 0:
            error = "Aucun résultat"
            if params_obj["key"] == "street_code":
                error += " pour le code de rue: "
            elif params_obj["key"] == "term":
                error += " pour l'adresse: "
            value = params_obj['value']
            error += f"'{value}'"
            return None, error
        elif result["items_total"] > 1:
            match, similarity_error = find_address_similarity(result["items"], params_obj["value"])
            if not match:
                error = "Plusieurs résultats"
                if params_obj["key"] == "street_code":
                    error += " pour le code de rue: "
                elif params_obj["key"] == "term":
                    error += " pour l'adresse: "
                value = params_obj['value']
                error += f"'{value}'"
                return None, error
            if similarity_error:
                return None, similarity_error
        else:
            match = result["items"][0]

        return match, None

    def transform_data(self, data):
        new_work_locations = []
        errors = []
        for worklocation in data["workLocations"]:
            base_params = {"match": self.search_match, "include_disable": self.seach_disable}
            params_list, error = self._generate_params(worklocation, data)
            if error:
                errors.append(error)
                continue

            error = None
            for params_obj in params_list:
                params = base_params
                params[params_obj["key"]] = params_obj["value"]

                result, error = self.request_params(params, params_obj)

                if error:
                    handle_error = params_obj.get("handle_error", None)
                    if handle_error:
                        data, action = handle_error(params_obj, worklocation, data)
                        if action == "break":
                            break
                        if action == "continue":
                            continue

                if result:
                    break

            if result:
                self._output_report(result["name"], worklocation)
                new_work_locations.append(
                    {
                        "street": result["uid"],
                        "number": worklocation.get("number", ""),
                    }
                )
            if error is not None:
                errors.append(error)

        data["workLocations"] = new_work_locations
        return data, errors


class TransformCadastre(core.GetFromRESTServiceInMemoryTask):
    browse_old_parcels = True
    mapping_division_dict = {}
    cadastre_key = "cadastre"

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

    def _mapping_division(self, divison):
        if divison not in self.mapping_division_dict:
            return "99999"
        return self.mapping_division_dict[divison]

    def transform_data(self, data):
        errors = []
        if self.cadastre_key not in data:
            return data, errors
        for cadastre in data[self.cadastre_key]:
            params, error = self._generate_cadastre_dict(cadastre, data)
            if error:
                errors.append(error)
                continue
            if params is None:
                continue
            params["browse_old_parcels"] = self.browse_old_parcels
            original_cadastre = params.pop("original_cadastre", None)
            r = self.request(parameters=params)
            if r.status_code != 200:
                errors.append(f"Response code is '{r.status_code}', expected 200")
                continue
            result = r.json()
            if result["items_total"] == 0:
                del params["browse_old_parcels"]
                error_res = f"Aucun résultat pour la parcelle '{params}'"
                if original_cadastre is not None:
                    error_res += f" (original : {original_cadastre})"
                errors.append(error_res)
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

    def handle_request_error(self, item, data):
        return data

    def handle_no_result(self, item, data):
        return data

    def handle_many_result(self, item, data):
        return data

    def transform_data(self, data):
        errors = []
        search, error = self._generate_contact_name(data)
        if search is None:
            return data, errors
        if error:
            errors.append(error)
            return data, errors
        if isinstance(search, str):
            search = [search]
        data[self.data_key] = []
        for contact in search:
            params = {"SearchableText": f"{contact}", "metadata_fields": "UID"}
            r = self.request(parameters=params)
            if r.status_code != 200:
                errors.append(f"Response code is '{r.status_code}', expected 200")
                data = self.handle_request_error(contact, data)
                continue
            result = r.json()
            if result["items_total"] == 0:
                errors.append(f"Aucun résultat pour: '{contact}'")
                data = self.handle_no_result(contact, data)
                continue
            elif result["items_total"] > 1:
                errors.append(f"Plusieurs résultats pour: '{contact}'")
                data = self.handle_many_result(contact, data)
                continue
            data[self.data_key].append(result["items"][0]["UID"])
        return data, errors
