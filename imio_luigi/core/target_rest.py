# -*- coding: utf-8 -*-

import abc
import luigi
import requests


class RESTTarget(luigi.Target):
    result = None

    _me_mapping = {
        "POST": requests.post,
        "PATCH": requests.patch,
        "DELETE": requests.delete,
        "PUT": requests.put,
    }

    def __init__(self, url, accept="application/json", login=None, password=None):
        self.url = url
        self.accept = accept
        self.login = login
        self.password = password

    @property
    @abc.abstractmethod
    def method(self):
        return None

    @property
    @abc.abstractmethod
    def expected_codes(self):
        return []

    @property
    def request_headers(self):
        headers = {"Accept": self.accept}
        if hasattr(self, "contenttype"):
            headers["Content-Type"] = self.contenttype
        return headers

    @property
    def request_kwargs(self):
        auth = None
        if self.login and self.password:
            auth = (self.login, self.password)
        kwargs = {"auth": auth, "headers": self.request_headers}
        if hasattr(self, "json_body"):
            kwargs["json"] = self.json_body
        if hasattr(self, "params"):
            kwargs["params"] = self.params
        return kwargs

    def request(self):
        """Perform the REST request"""
        me = self._me_mapping.get(self.method)
        result = me(self.url, **self.request_kwargs)
        if not hasattr(result, "status_code"):
            raise RuntimeError("Request result has no status code")
        if result.status_code not in self.expected_codes:
            message = (
                "Wrong request returned status_code {0}, expected codes are: {1}, "
                "message: {2}"
            )
            raise RuntimeError(
                message.format(
                    result.status_code,
                    ", ".join([str(c) for c in self.expected_codes]),
                    result.content,
                )
            )
        return result

    def _test_content(self):
        """Perform a REST request to validate the target"""
        return

    def open(self, mode):
        raise NotImplementedError("Cannot open() RESTTarget")

    def exists(self):
        raise NotImplementedError("Must be defined in tasks")


class PostRESTTarget(RESTTarget):
    method = "POST"
    contenttype = "application/json"
    expected_codes = [201]

    def __init__(self, url, json_body, **kwargs):
        self.json_body = json_body
        super(PostRESTTarget, self).__init__(url, **kwargs)


class PatchRESTTarget(RESTTarget):
    method = "PATCH"
    contenttype = "application/json"
    expected_codes = [200, 204]

    def __init__(self, url, json_body, **kwargs):
        self.json_body = json_body
        super(PatchRESTTarget, self).__init__(url, **kwargs)


class DeleteRESTTarget(RESTTarget):
    method = "DELETE"
    expected_codes = [200, 204]
    contenttype = "application/json"

    def __init__(self, url, json_body, **kwargs):
        self.json_body = json_body
        super(DeleteRESTTarget, self).__init__(url, **kwargs)


class PutRESTTarget(RESTTarget):
    method = "PUT"
    expected_codes = [200, 204]
