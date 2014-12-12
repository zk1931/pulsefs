import json
import logging
import pytest
import requests
import time
import threading
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
        assert res.headers["type"] == "dir"
        body = json.loads(res.content)
        assert body["path"] == "/"
        assert body["type"] == "dir"

        # make sure checksum is a 32-bit int in zero-padded hex, and the
        # checksum in the header and the checksum in the body match.
        assert len(res.headers["checksum"]) == 8
        int(res.headers["checksum"], 16)
        assert len(body["checksum"]) == 8
        int(body["checksum"], 16)
        assert res.headers["checksum"] == body["checksum"]
        assert body["children"][0]["path"] == "/pulsed"

    def test_encoded_url(self):
        directory = "/" + str(uuid.uuid4())

        # create a directory
        requests.put(self.baseurl + directory + "?dir")

        # create a file with an url-encoded filename
        res = requests.put(self.baseurl + directory + "/%00", "test")
        res = requests.get(self.baseurl + directory + "/%00")
        assert res.content == "test"

        # make sure the filename in directory listing remains url-encoded.
        res = requests.get(self.baseurl + directory)
        body = json.loads(res.content)
        assert body["children"][0]["path"] == directory + "/%00"

    def test_server_header(self):
        """verify that the server header is not present"""
        res = requests.get(self.baseurl)
        assert "Server" not in res.headers

    def test_return_code(self):
        directory = "/" + str(uuid.uuid4())

        # create a directory
        res = requests.put(self.baseurl + directory + "?dir")
        assert res.status_code == 201
        res = requests.put(self.baseurl + directory + "?dir")
        # better to return 409
        assert res.status_code == 400
        assert res.reason == directory + " already exists"

        # create a file
        res = requests.put(self.baseurl + directory + "/file", "test")
        assert res.status_code == 201

        # update the file
        res = requests.put(self.baseurl + directory + "/file", "test")
        assert res.status_code == 200

    def test_recursive_delete(self):
        directory = "/" + str(uuid.uuid4())

        # create a directory
        res = requests.put(self.baseurl + directory + "?dir")
        assert res.status_code == 201

        res = requests.put(self.baseurl + directory + "/a/b/c/d?recursive")
        assert res.status_code == 201
        assert res.reason == "Created"

        res = requests.delete(self.baseurl + directory + "/a")
        assert res.status_code == 400
        assert res.reason == directory + "/a is not empty"

        res = requests.delete(self.baseurl + directory + "/a?recursive")
        assert res.status_code == 200
        assert res.reason == "OK"

        res = requests.delete(self.baseurl + directory + "/a")
        assert res.status_code == 404
        assert res.reason == directory + "/a does not exist"

    def test_create_child_on_file(self):
        directory = "/" + str(uuid.uuid4())

        # create a directory
        res = requests.put(self.baseurl + directory + "?dir")
        assert res.status_code == 201
        assert res.reason == "Created"

        res = requests.put(self.baseurl + directory + "/file")
        assert res.status_code == 201
        assert res.reason == "Created"

        res = requests.put(self.baseurl + directory + "/file/child")
        assert res.status_code == 400
        assert res.reason == directory + "/file is not a directory"

    def test_put_on_dir(self):
        res = requests.put(self.baseurl + "/", "test")
        assert res.status_code == 400
        assert res.reason == "/ is a directory"

    def test_wait_create(self):
        directory = "/" + str(uuid.uuid4())
        threads = []
        results = []

        def get(url):
            results.append(requests.get(url))

        # wait for directory creation
        for i in range(0, 3):
            url = self.baseurl + directory + "?wait=0"
            thread = threading.Thread(target=get, args=[url])
            thread.start()
            threads.append(thread)

        # create a directory
        res = requests.put(self.baseurl + directory + "?dir")
        assert res.status_code == 201
        assert res.reason == "Created"

        for thread in threads:
            thread.join()

        assert results[0].status_code == 200
        assert results[0].headers["version"] == "0"
        assert results[0].headers["type"] == "dir"
        assert all(r.status_code == results[0].status_code for r in results)
        assert all(r.headers["checksum"] ==
                   results[0].headers["checksum"] for r in results)
        assert all(r.content == results[0].content for r in results)

    def test_wait_file(self):
        directory = "/" + str(uuid.uuid4())
        threads = []
        results = []

        # create a file
        res = requests.put(self.baseurl + directory + "/file?recursive")
        assert res.status_code == 201
        assert res.reason == "Created"

        def get(url):
            results.append(requests.get(url))

        # wait for version 10
        for i in range(0, 3):
            url = self.baseurl + directory + "/file?wait=10"
            thread = threading.Thread(target=get, args=[url])
            thread.start()
            threads.append(thread)

        for i in range(1, 11):
            requests.put(self.baseurl + directory + "/file", str(i))

        for thread in threads:
            thread.join()

        assert results[0].status_code == 200
        assert results[0].headers["version"] == "10"
        assert results[0].headers["type"] == "file"
        assert results[0].content == "10"
        assert all(r.status_code == results[0].status_code for r in results)
        assert all(r.headers == results[0].headers for r in results)
        assert all(r.content == results[0].content for r in results)

    def test_put_version(self):
        directory = "/" + str(uuid.uuid4())
        res = requests.put(self.baseurl + directory + "?dir")
        assert res.status_code == 201
        assert res.reason == "Created"
        assert res.headers["version"] == "0"

        # version is -1, create a file /bar.
        res = requests.put(self.baseurl + directory + "/bar?version=-1")
        assert res.status_code == 201
        assert res.reason == "Created"

        # version is 0, set file /foo.
        res = requests.put(self.baseurl + directory + "/foo?version=0")
        assert res.status_code == 404
        assert res.reason == directory + "/foo does not exist"

        # version is 0, set file /bar.
        res = requests.put(self.baseurl + directory + "/bar?version=0")
        assert res.status_code == 200
        assert res.headers["version"] == "1"

        # version is 1, set file /bar.
        res = requests.put(self.baseurl + directory + "/bar?version=0")
        assert res.status_code == 409
        assert res.reason == "Version %s doesn't match node version %s" % (0,
                                                                           1)

        # set with valid version
        res = requests.put(self.baseurl + directory + "/bar?version=1")
        assert res.status_code == 200

    def test_delete_version(self):
        directory = "/" + str(uuid.uuid4())
        res = requests.put(self.baseurl + directory + "?dir")
        assert res.status_code == 201
        assert res.reason == "Created"
        assert res.headers["version"] == "0"

        # version is -1, create a file /bar.
        res = requests.put(self.baseurl + directory + "/bar?version=-1")
        assert res.status_code == 201
        assert res.reason == "Created"
        assert res.headers["version"] == "0"

        # delete it with wrong version
        res = requests.delete(self.baseurl + directory + "/bar?version=1")
        assert res.status_code == 409
        assert res.reason == "Version %s doesn't match node version %s" % (1,
                                                                           0)

        # delete it with correct version
        res = requests.delete(self.baseurl + directory + "/bar?version=0")
        assert res.status_code == 200

    def test_delete_while_wait(self):
        directory = "/" + str(uuid.uuid4())
        res = requests.put(self.baseurl + directory + "?dir")
        assert res.status_code == 201
        assert res.reason == "Created"
        assert res.headers["version"] == "0"

        # wait the version of /bar becomes 2 while /bar doesn't exist.
        res = requests.get(self.baseurl + directory + "/bar?wait=2")
        # we should return 404
        assert res.status_code == 404
        assert res.reason == directory + "/bar does not exist"
        results = []

        def get(url):
            results.append(requests.get(url))

        url = self.baseurl + directory + "/bar?wait=0"
        thread = threading.Thread(target=get, args=[url])
        thread.start()
        # create /bar
        res = requests.put(self.baseurl + directory + "/bar")
        # wati watch returns.
        thread.join()
        # watch returns 200 success.
        assert results[0].status_code == 200
        assert results[0].headers["version"] == "0"

        # wait version of /bar becomes 2.
        url = self.baseurl + directory + "/bar?wait=2"
        thread = threading.Thread(target=get, args=[url])
        thread.start()

        # delete /bar
        res = requests.delete(self.baseurl + directory + "/bar")
        thread.join()
        # watch returns 404 not found.
        assert results[1].status_code == 404

    def test_sequential(self):
        directory = "/" + str(uuid.uuid4())
        res = requests.put(self.baseurl + directory + "?dir")
        assert res.status_code == 201
        assert res.reason == "Created"
        assert res.headers["version"] == "0"

        res = requests.post(self.baseurl + directory)
        assert res.status_code == 201
        assert res.reason == "Created"
        assert res.headers["Location"] is not None
        path1 = res.headers["Location"]

        res = requests.post(self.baseurl + directory)
        assert res.status_code == 201
        assert res.reason == "Created"
        assert res.headers["Location"] is not None
        path2 = res.headers["Location"]

        assert path2 > path1

    def test_validate_query(self):
        res = requests.get(self.baseurl)
        assert res.status_code == 200
        version = res.headers["version"]

        res = requests.get(self.baseurl + "/?wait" + str(version))
        assert res.status_code == 200

        # invalid query.
        res = requests.get(self.baseurl + "/?wait")
        # bad query parameter
        assert res.status_code == 400

        # invalid query.
        res = requests.get(self.baseurl + "/?wait=abc")
        # bad query parameter
        assert res.status_code == 400

    def test_transient_node(self):
        directory = "/" + str(uuid.uuid4())
        res = requests.put(self.baseurl + directory + "?dir")
        assert res.status_code == 201
        assert res.reason == "Created"
        assert res.headers["version"] == "0"

        file1 = self.baseurl + directory + "/foo/bar/file1"
        file2 = self.baseurl + directory + "/foo/bar/file2"
        # create file1
        res = requests.put(file1 + "?recursive&transient")
        assert res.status_code == 201
        assert res.reason == "Created"

        # create file2
        res = requests.put(file2 + "?recursive&transient")
        assert res.status_code == 201
        assert res.reason == "Created"

        res = requests.get(self.baseurl + directory + "/foo/bar")
        assert res.status_code == 200
        # Make sure it's transient node.
        assert res.headers["type"] == "transient-dir"

        res = requests.get(self.baseurl + directory + "/foo")
        assert res.status_code == 200
        # Make sure it's transient node.
        assert res.headers["type"] == "transient-dir"

        # delete file1
        res = requests.delete(file1)
        assert res.status_code == 200

        # make sure file1 gets deleted
        requests.get(file1).status_code = 404
        # make sure file2 still exists
        requests.get(file2).status_code = 200
        # make sure /foo/bar still exists
        requests.get(self.baseurl + directory + "/foo/bar").status_code = 200
        # make sure /foo still exists
        requests.get(self.baseurl + directory + "/foo").status_code = 200

        # delete file2
        res = requests.delete(file2)
        assert res.status_code == 200

        # make sure file2 gets deleted
        requests.get(file2).status_code = 404
        # make sure /foo/bar gets deleted since it has no child anymore.
        requests.get(self.baseurl + directory + "/foo/bar").status_code = 404
        # make sure /foo gets deleted since it has no child anymore.
        requests.get(self.baseurl + directory + "/foo").status_code = 404

    def test_pulsed_servers(self):
        directory = "/pulsed/servers"
        res = requests.get(self.baseurl + directory)
        # make sure directory /pulsed/servers exists
        assert res.status_code == 200
        body = json.loads(res.content)
        assert body["path"] == "/pulsed/servers"
        assert body["children"][0]["path"] == directory + "/localhost:5000"

        res = requests.put(self.baseurl + directory + "/file")
        # can't create file under /pulsed/servers
        assert res.status_code == 403
        assert res.reason == "Forbidden"

        res = requests.delete(self.baseurl + directory + "/localhost:5000")
        # can't delete file under /pulsed/servers
        assert res.status_code == 403
        assert res.reason == "Forbidden"
