# -*- coding: utf-8 -*-

from imio_luigi import core

import abc


class MappingCountryTask(core.MappingValueTask):
    mapping = {
        "BE": "belgium",
    }


class MappingCountryInMemoryTask(MappingCountryTask, core.MappingValueInMemoryTask):
    pass
