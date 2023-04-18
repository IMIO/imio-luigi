# -*- coding: utf-8 -*-

import re


def extract_cadastre(value):
    """Extract cadastre informations from a string"""
    value = value.strip()
    value = value.replace(" ", "")

    def _return_value(value, counter, puissance=False):
        partie = False
        for key in ("(pie)", "(partie)", "partie", "parties", "pie", "PIE"):
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

    if re.match("^\d{3}\w{1}(partie|parties|pie|PIE|\(pie\)|\(partie\)){0,1}$", value):
        return _return_value(value, 3)
    if re.match("^\d{2}\w{1}(partie|parties|pie|PIE|\(pie\)|\(partie\)){0,1}$", value):
        return _return_value(value, 2)
    if re.match(
        "^\d{3}\w{1}\d{1,2}(partie|parties|pie|PIE|\(pie\)|\(partie\)){0,1}$", value
    ):
        return _return_value(value, 3, puissance=True)
    if re.match(
        "^\d{2}\w{1}\d{1,2}(partie|parties|pie|PIE|\(pie\)|\(partie\)){0,1}$", value
    ):
        return _return_value(value, 2, puissance=True)
    raise ValueError(f"Can not parse value '{value}'")
