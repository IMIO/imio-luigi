# -*- coding: utf-8 -*-

from imio_luigi.core.target_rest import (
    DeleteRESTTarget,
    PatchRESTTarget,
    PostRESTTarget,
)

import abc
import luigi
import requests


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
            self.url,
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
            self.url,
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
            self.url, self.json_body, login=self.login, password=self.password
        )
