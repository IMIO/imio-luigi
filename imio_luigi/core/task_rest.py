# -*- coding: utf-8 -*-

from imio_luigi.core.target_rest import (
    DeleteRESTTarget,
    PatchRESTTarget,
    PostRESTTarget,
)
from imio_luigi.core.utils import mock_filename
from luigi.mock import MockTarget
from luigi.parameter import ParameterVisibility

import abc
import json
import luigi
import requests


class GetFromRESTServiceTask(luigi.Task):
    method = "GET"
    url = luigi.Parameter()
    parameters = luigi.OptionalParameter(default=None)
    accept = luigi.OptionalParameter(default="application/json")
    login = luigi.OptionalParameter(default="")
    password = luigi.OptionalParameter(
        default="", visibility=ParameterVisibility.PRIVATE
    )

    _me_mapping = {
        "GET": requests.get,
        "POST": requests.post,
        "PATCH": requests.patch,
        "DELETE": requests.delete,
        "PUT": requests.put,
    }

    @property
    def request_url(self):
        """Method that can be overrided if necessary to construct the request URL"""
        return self.url

    def request(self, parameters=None):
        """Perform the REST request"""
        me = self._me_mapping.get(self.method)
        if parameters is None:
            parameters = self.parameters
        auth = None
        if self.login and self.password:
            auth = (self.login, self.password)
        return me(
            self.request_url,
            headers={"Accept": self.accept},
            auth=auth,
            params=parameters,
        )

    @property
    @abc.abstractmethod
    def key(self):
        """The unique key"""
        return None

    @abc.abstractmethod
    def transform_data(self, data):
        """Method that need to be overrided to define transformation"""
        return None

    def run(self):
        with self.input().open("r") as input_f:
            with self.output().open("w") as output_f:
                data = json.load(input_f)
                json.dump(self.transform_data(data), output_f)


class GetFromRESTServiceInMemoryTask(GetFromRESTServiceTask):
    def output(self):
        return MockTarget(
            mock_filename(self, "GetFromRESTService"), mirror_on_stderr=True
        )


class BaseRESTTask(luigi.Task):
    """Baseclass for task based on REST"""

    @property
    @abc.abstractmethod
    def method(self):
        return None

    @property
    @abc.abstractmethod
    def url(self):
        return None

    @property
    @abc.abstractmethod
    def test_url(self):
        return None

    @property
    @abc.abstractmethod
    def accept(self):
        return None

    @property
    @abc.abstractmethod
    def login(self):
        return None

    @property
    @abc.abstractmethod
    def password(self):
        return None

    @property
    @abc.abstractmethod
    def test_parameters(self):
        return None

    @abc.abstractmethod
    def complete(self):
        raise NotImplementedError("Complete must be defined")

    @property
    def request_url(self):
        """Method that can be overrided if necessary to construct the request URL"""
        return self.url

    def test_complete(self):
        """
        Method that make a request to validate that the change was done.
        Return the request result or None if the returned status_code is not 200
        """
        auth = None
        if self.login and self.password:
            auth = (self.login, self.password)
        r = requests.get(
            self.test_url,
            headers={"Accept": self.accept},
            auth=auth,
            params=self.test_parameters,
        )
        if not hasattr(r, "status_code"):
            raise RuntimeError("Request result has no status code")
        if r.status_code != 200:
            message = (
                "Wrong request returned status_code {0}, expected codes is: 200, "
                "message: {1}"
            )
            raise RuntimeError(
                message.format(
                    r.status_code,
                    r.content,
                )
            )
        return r


class PostRESTTask(BaseRESTTask):
    method = "POST"
    accept = "application/json"

    @property
    @abc.abstractmethod
    def json_body(self):
        return None

    def output(self):
        return PostRESTTarget(
            self.request_url,
            self.json_body,
            login=self.login,
            password=self.password,
        )


class PatchRESTTask(BaseRESTTask):
    method = "PATCH"
    accept = "application/json"

    @property
    @abc.abstractmethod
    def json_body(self):
        return None

    def output(self):
        return PatchRESTTarget(
            self.request_url,
            self.json_body,
            login=self.login,
            password=self.password,
        )


class DeleteRESTTask(BaseRESTTask):
    method = "DELETE"
    accept = "application/json"

    @property
    @abc.abstractmethod
    def json_body(self):
        return None

    def output(self):
        return DeleteRESTTarget(
            self.request_url, self.json_body, login=self.login, password=self.password
        )
