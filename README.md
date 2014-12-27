PulseFS [![Build Status](https://travis-ci.org/zk1931/pulsefs.svg?branch=master)](https://travis-ci.org/zk1931/pulsefs)
=====

`PulseFS` is an HTTP-based replicated filestore for distributed coordination.
Its features include:

- **Sequentially consistent** All the replicas see updates in the same order.
- **Ephemeral files and transient directories** for implementing distributed
  coordination primitives such as locks and barriers.
- **Sequential files** for ordering writes.
- **Dynamic cluster reconfiguration** Add/remove servers without restarting the
  cluster.
- **Atomic updates** Atomically execute multiple operations.

Getting Started
---------------

- [API specifications](https://github.com/zk1931/pulsefs/blob/master/SPEC.md)
  describes the API in details.

Compilation
-----------

    mvn clean compile assembly:single

Usage
-----
To start a cluster, run:

    ./bin/pulsefs -port 8080 -addr localhost:5000
    ./bin/pulsefs -port 8081 -addr localhost:5001 -join localhost:5000
    ./bin/pulsefs -port 8082 -addr localhost:5002 -join localhost:5000
