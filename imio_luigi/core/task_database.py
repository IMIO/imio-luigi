# -*- coding: utf-8 -*-

from imio_luigi.core.utils import mock_filename
from luigi.mock import MockTarget
from urllib.parse import quote_plus

import abc
import json
import luigi
import sqlalchemy


class GetFromDatabaseTask(luigi.Task):
    columns = ["*"]

    @property
    @abc.abstractmethod
    def connector(self):
        return None

    @property
    @abc.abstractmethod
    def url(self):
        return None

    @property
    @abc.abstractmethod
    def tablename(self):
        return None

    def check_if_table_exist(self, connection):
        query = (
            "select count(*) "
            "from information_schema.tables "
            f"where table_schema = DATABASE() AND table_name = '{self.tablename}';"
        )
        result = connection.execute(sqlalchemy.text(query)).fetchall()
        return result[0][0] > 0

        columns = ",".join(self.columns)
        query = f"select {columns} from {self.tablename}"
        if limit:
            query += f" limit {limit}"
        if offset:
            query += f" offset {offset}"
        return query

    def query(self, limit=None, offset=None):
        engine = sqlalchemy.create_engine(self.url)
        with engine.connect() as connection:
            if not self.check_if_table_exist(connection):
                return []
        return result

    def log_failure_output(self):
        fname = self.task_id.split("_")[-1]
        fpath = (
            f"./failures/{self.task_namespace}-"
            f"{self.__class__.__name__}/{fname}.json"
        )
        return luigi.LocalTarget(fpath)


class GetFromMySQLTask(GetFromDatabaseTask):
    encoding = None
    connector = "mysql+pymysql"
    login = luigi.Parameter()
    password = luigi.Parameter()
    host = luigi.Parameter()
    port = luigi.IntParameter()
    dbname = luigi.Parameter()

    @property
    def url(self):
        return "{connector}://{login}:{password}@{host}:{port}/{dbname}".format(
            connector=self.connector,
            login=self.login,
            password=quote_plus(self.password),
            host=self.host,
            port=self.port,
            dbname=self.dbname,
        )


class JoinFromMySQLTask(GetFromMySQLTask):
    destination = luigi.Parameter()

    @abc.abstractmethod
    def sql_condition(self):
        return None

    def sql_query(self, limit=None, offset=None):
        columns = ",".join(self.columns)
        query = f"select {columns} from {self.tablename} where {self.sql_condition()}"
        if limit:
            query += f" limit {limit}"
        if offset:
            query += f" offset {offset}"
        return query

    def hook_before_serialization(self, data):
        return data

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                rows = [{k: getattr(r, k) for k in r._fields} for r in self.query()]
                rows = self.hook_before_serialization(rows)
                if self.destination not in data:
                    if self.destination_type == "array":
                    data[self.destination] = []    
                    elif self.destination_type == "dict":
                        data[self.destination] = {}
                if self.destination_type == "array":
                data[self.destination] = data[self.destination] + rows
                elif self.destination_type == "dict":
                    data[self.destination] = data[self.destination] | rows

                json.dump(data, output_f)


class JoinFromMySQLInMemoryTask(JoinFromMySQLTask):
    def output(self):
        return MockTarget(mock_filename(self, "JoinFromMySQL"), mirror_on_stderr=True)
