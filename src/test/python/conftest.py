import json
import logging
import os
import pytest
import requests
import shutil
import subprocess
import time

logging.basicConfig(level=logging.INFO)


def get_executable():
    path = os.path.realpath(__file__)
    for i in range(0, 4):
        path = os.path.dirname(path)
    return os.path.join(path, "bin", "pulsefs")


@pytest.fixture(scope="class")
def single_server(request):
    logging.info("setting up single server")
    executable = get_executable()
    directory = "localhost:5000"
    request.cls.baseurl = "http://localhost:8080"
    args = [executable, "-port", "8080", "-addr", "localhost:5000",
            "-timeout", "3"]
    process = subprocess.Popen(args)
    for i in range(0, 10):
        try:
            requests.get(request.cls.baseurl)
            logging.info("single server started on localhost:8080 ")
            break
        except:
            time.sleep(1)

    @request.addfinalizer
    def teardown():
        logging.info("tearing down single server")
        process.terminate()
        shutil.rmtree(directory)


@pytest.fixture(scope="class")
def multiple_servers(request):
    logging.info("setting up multiple servers")
    exe = get_executable()
    zab_server1 = "localhost:5001"
    zab_server2 = "localhost:5002"
    zab_server3 = "localhost:5003"
    server1 = "http://localhost:8081"
    server2 = "http://localhost:8082"
    server3 = "http://localhost:8083"
    directories = [zab_server1, zab_server2, zab_server3]
    request.cls.server1 = server1
    request.cls.server2 = server2
    request.cls.server3 = server3
    args1 = [exe, "-port", "8081", "-addr", zab_server1, "-timeout", "3"]
    args2 = [exe, "-port", "8082", "-addr", zab_server2, "-join", zab_server1,
             "-timeout", "3"]
    args3 = [exe, "-port", "8083", "-addr", zab_server3, "-join", zab_server1,
             "-timeout", "3"]

    process1 = subprocess.Popen(args1)
    process2 = subprocess.Popen(args2)
    process3 = subprocess.Popen(args3)

    for i in range(0, 10):
        try:
            res = requests.get(server1 + "/pulsefs/servers")
            body = json.loads(res.content)
            if len(body["children"]) != 3:
                raise "Not all servers are up"
            res = requests.get(server2 + "/pulsefs/servers")
            body = json.loads(res.content)
            if len(body["children"]) != 3:
                raise "Not all servers are up"
            res = requests.get(server3 + "/pulsefs/servers")
            body = json.loads(res.content)
            if len(body["children"]) != 3:
                raise "Not all servers are up"
            logging.info("all servers started")
            break
        except:
            time.sleep(1)

    @request.addfinalizer
    def teardown():
        logging.info("tearing down multiple servers")
        process1.terminate()
        process2.terminate()
        process3.terminate()
        for directory in directories:
            shutil.rmtree(directory)
