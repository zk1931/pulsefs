pulsed [![Build Status](https://travis-ci.org/ZK-1931/pulsed.svg?branch=master)](https://travis-ci.org/ZK-1931/pulsed)
=====

A reference implementation for using [javazab](https://github.com/ZK-1931/javazab)

Usage
-----
To start a cluster, run:

    ./bin/pulsed 8080 -DserverId=localhost:5000
    ./bin/pulsed 8081 -DserverId=localhost:5001 -Djoin=localhost:5000
    ./bin/pulsed 8082 -DserverId=localhost:5002 -Djoin=localhost:5001
