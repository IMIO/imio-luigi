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
