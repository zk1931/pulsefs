pulsed [![Build Status](https://travis-ci.org/zk1931/pulsed.svg?branch=master)](https://travis-ci.org/zk1931/pulsed)
=====

A reference implementation for using [jzab](https://github.com/zk1931/jzab)

Compilation
-----------

    mvn clean compile assembly:single

Usage
-----
To start a cluster, run:

    ./bin/pulsed -port 8080 -addr localhost:5000
    ./bin/pulsed -port 8081 -addr localhost:5001 -join localhost:5000
    ./bin/pulsed -port 8082 -addr localhost:5002 -join localhost:5000
