import json
import logging
import pytest
import requests
import uuid


@pytest.mark.usefixtures("single_server")
class TestSingleServer(object):
    """Single server tests.

    Base URL of the server (e.g. http://localhost:8080) can be accessed via
    self.baseurl attribute.
    """

    @pytest.mark.first
    def test_verify_initial_state(self):
        res = requests.get(self.baseurl)
        assert res.headers["path"] == "/"
        assert res.headers["type"] == "dir"
        assert res.headers["version"] == "0"
        body = json.loads(res.content)
        assert body["children"] == []
        assert body["path"] == "/"
        assert body["type"] == "dir"
        assert body["version"] == 0

        # make sure checksum is a 32-bit int in zero-padded hex, and the
        # checksum in the header and the checksum in the body match.
        assert len(res.headers["checksum"]) == 8
        int(res.headers["checksum"], 16)
        assert len(body["checksum"]) == 8
        int(body["checksum"], 16)
        assert res.headers["checksum"] == body["checksum"]

    def test_encoded_url(self):
        directory = "/" + str(uuid.uuid4())

        # create a directory
        requests.put(self.baseurl + directory + "?dir")

        # create a file with an url-encoded filename
        res = requests.put(self.baseurl + directory + "/%00", "test")
        res = requests.get(self.baseurl + directory + "/%00")
        assert res.headers["path"] == directory + "/%00"
        assert res.content == "test"

        # make sure the filename in directory listing remains url-encoded.
        res = requests.get(self.baseurl + directory)
        body = json.loads(res.content)
        assert body["children"][0]["path"] == directory + "/%00"

    def test_server_header(self):
        """verify that the server header is not present"""
        res = requests.get(self.baseurl)
        assert "Server" not in res.headers
