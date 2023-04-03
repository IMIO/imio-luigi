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

    @property
    def sql_query(self):
        columns = ",".join(self.columns)
        return f"select {columns} from {self.tablename}"

    def query(self):
        engine = sqlalchemy.create_engine(self.url)
        with engine.connect() as connection:
            result = connection.execute(sqlalchemy.text(self.sql_query)).fetchall()
        return result


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

    @property
    def sql_query(self):
        columns = ",".join(self.columns)
        return f"select {columns} from {self.tablename} where {self.sql_condition()}"

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                rows = [{k: getattr(r, k) for k in r._fields} for r in self.query()]
                data[self.destination] = rows
                json.dump(data, output_f)


class JoinFromMySQLInMemoryTask(JoinFromMySQLTask):
    def output(self):
        return MockTarget(mock_filename(self, "JoinFromMySQL"), mirror_on_stderr=True)
