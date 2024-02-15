# -*- coding: utf-8 -*-

from datetime import datetime
from imio_luigi import core, utils
from imio_luigi.urban import core as ucore
from imio_luigi.urban import tools
from imio_luigi.urban.address import find_address_match

import copy
import csv
import json
import logging
import luigi
import numpy as np
import os
import pandas as pd
import re


logger = logging.getLogger("luigi-interface")


class GetFromCSV(core.GetFromCSVFile):
    task_namespace = "renaming"
    filepath = luigi.Parameter()
    export_filepath = luigi.Parameter()
    ref_key = luigi.Parameter()
    line_range = luigi.Parameter(default=None)
    counter = luigi.Parameter(default=None)
    export_only_doublon = luigi.BoolParameter()
    delimiter = ";"
    dtype = "string"

    def output(self):
        return luigi.local_target.LocalTarget(path=self.export_filepath)

    def run(self):
        min_range = None
        max_range = None
        counter = None
        if self.line_range:
            if not re.match(r"\d{1,}-\d{1,}", self.line_range):
                raise ValueError("Wrong Line Range")
            line_range = self.line_range.split("-")
            min_range = int(line_range[0])
            max_range = int(line_range[1])
        if self.counter:
            counter = int(self.counter)
        iteration = 0
        output = []
        ref = {}
        for row in self.query(min_range=min_range, max_range=max_range):
            if row[self.ref_key] in ref:
                ref[row[self.ref_key]] += 1
                row[self.ref_key] += f"_{ref[row[self.ref_key]]}"
                if self.export_only_doublon:
                    output.append(row)
            else:
                ref[row[self.ref_key]] = 0

            if not self.export_only_doublon:
                output.append(row)

            iteration += 1
            if counter and iteration >= counter:
                break

        df = pd.DataFrame(output, dtype="string")
        df = df.replace({np.nan: None})

        df.to_csv(self.export_filepath, sep=";", quoting=csv.QUOTE_NONNUMERIC)
        print_output = [ref for ref, value in ref.items() if value > 0]
        print(f"{len(print_output)} permis renomé :")
        print(f"{', '.join(print_output)}")
