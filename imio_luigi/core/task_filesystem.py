# -*- coding: utf-8 -*-

import abc
import json
import luigi
import os


class WalkFS(luigi.Task):
    @property
    @abc.abstractmethod
    def path(self):
        """The path to explore"""
        return None

    @property
    @abc.abstractmethod
    def extension(self):
        """The extension of files"""
        return None

    @property
    def filepaths(self):
        """Generator that return every file paths in the specified directory"""
        for folder, dirnames, filenames in os.walk(self.path):
            for fname in filenames:
                yield os.path.join(folder, fname)


class WriteToJSONTask(luigi.Task):
    @property
    @abc.abstractmethod
    def key(self):
        """The unique key that will be used for the filename"""
        return None

    @property
    @abc.abstractmethod
    def export_filepath(self):
        """The directory where file will be written"""
        return None

    @property
    def filepath(self):
        """Computed filepath based on the export directory filepath and the key"""
        key = self.key
        if key.startswith("/"):
            key = key[1:]
        replacements = {
            "/": "-",
            " ": "_",
            "*": "-",
        }

        for k, v in replacements.items():
            if k in key:
                key = key.replace(k, v)
        return os.path.join(self.export_filepath, f"{key}.json")

    def output(self):
        return luigi.local_target.LocalTarget(path=self.filepath)

    def run(self):
        data = None
        with self.input().open(mode="r") as input_f:
            with self.output().open(mode="w") as output_f:
                data = json.load(input_f)
                json.dump(data, output_f)

    def complete(self):
        return self.output().exists()
