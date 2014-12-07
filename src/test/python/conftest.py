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
    return os.path.join(path, "bin", "pulsed")


@pytest.fixture(scope="class")
def single_server(request):
    logging.info("setting up single server")
    executable = get_executable()
    directory = "localhost:5000"
    request.cls.baseurl = "http://localhost:8080"
    args = [executable, "-port", "8080", "-addr", "localhost:5000"]
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
