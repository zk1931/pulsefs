import json
import logging
import pytest
import requests
import time
import threading
import uuid


@pytest.mark.usefixtures("multiple_servers")
class TestMultipleServers(object):
    """Multiple servers tests.

    Starts 3 servers, the url of the  3 servers can be accessed via
    self.server1, self.server2, self.server3
    """

    def assert_consistency(self):
        res1 = requests.get(self.server1)
        res2 = requests.get(self.server2)
        res3 = requests.get(self.server3)
        assert res1.status_code == 200
        assert res2.status_code == 200
        assert res3.status_code == 200
        # making sure the states of the 3 servers are consistent.
        assert res1.headers["checksum"] == res2.headers["checksum"]
        assert res3.headers["checksum"] == res2.headers["checksum"]
        assert res1.headers["version"] == res2.headers["version"]
        assert res3.headers["version"] == res2.headers["version"]

    @pytest.mark.first
    def test_verify_initial_state(self):
        self.assert_consistency()

    def test_create_directory(self):
        directory = "/" + str(uuid.uuid4())

        # creating a directory on server1
        res = requests.put(self.server1 + directory + "?dir")
        assert res.status_code == 201
        # waiting this directory to be created on server 2 and server3.
        res = requests.get(self.server2 + directory + "?wait=0")
        res = requests.get(self.server3 + directory + "?wait=0")
        # consistency check
        self.assert_consistency()
