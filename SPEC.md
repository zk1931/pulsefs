spec draft
==========

Pulsed has two types of entities, directories and files. Directories can contain
other directories and files, and files contain data, but it cannot contain other
directories or files.

TODO: Add a paragraph explaining about sessions.

There are two kinds of directories, regular and transient. Regular directories
are allowed to be empty. Transient directories automatically get deleted when
they become empty.

There are two kinds of files, regular and ephemeral. Regular files remains in
pulsed until they explicitly get deleted by the user. Each ephemeral file is
associated with a session, and it gets deleted automatically when the session
expires.

A special directory `/pulsed` is reserved for use by pulsed.

creating a new regular file
---------------------------

    PUT /newfile HTTP/1.1
    content-length: 13

    Hello, world!

    HTTP/1.1 200 OK
    version: 0
    content-length: 0

updating an existing regular file
---------------------------------

    PUT /newfile HTTP/1.1
    content-length: 21

    Hello, updated world!

    HTTP/1.1 200 OK
    version: 1
    content-length: 0

deleting an existing regular file
---------------------------------

    DELETE /newfile HTTP/1.1

    HTTP/1.1 200 OK
    content-length: 0

creating a new regular directory
--------------------------------

    PUT /newdir?dir HTTP/1.1
    content-length: 0

    HTTP/1.1 200 OK
    version: 0
    content-length: 0

creating a new transient directory
----------------------------------

Transient directories get created implicitly when you create a file in a
non-existing location.

    PUT /foo/bar/file HTTP/1.1
    content-length: 13

    Hello, world!

    HTTP/1.1 200 OK
    version: 0
    content-length: 0

Assuming /foo and /foo/bar didn't exist, they get created as a transient
directories. When /foo/bar/file gets deleted, these directories get deleted
automatically.

creating a sequential file in a directory
-----------------------------------------

You can create files with strictly increasing filenames (sequential files) by
sending POST requests to directories.

    POST /foo/bar/dir HTTP/1.1
    content-length: 5

    First

    HTTP/1.1 200 OK
    location: /foo/bar/dir/0000000000000000
    version: 0
    content-length: 0

    POST /foo/bar/dir HTTP/1.1
    content-length: 6

    Second

    HTTP/1.1 200 OK
    location: /foo/bar/dir/0000000000000001
    version: 0
    content-length: 0

listing a directory
-------------------
TBD

deleting a directory
--------------------

    DELETE /newdir HTTP/1.1

    HTTP/1.1 200 OK
    content-length: 0

Note that the directory must be empty or else DELETE request will fail. To delete
a directory recursively:

    DELETE /newdir?recursive HTTP/1.1

    HTTP/1.1 200 OK
    content-length: 0

watching a file or a directory
------------------------------
TBD

creating a session
------------------

    POST /pulsed/session HTTP/1.1

    HTTP/1.1 200 OK
    content-length: 0
    location: /pulsed/session/0000000000000000

deleting a session
------------------

    DELETE /pulsed/session/0000000000000000 HTTP/1.1

    HTTP/1.1 200 OK
    content-length: 0

executing multiple operations atomically
----------------------------------------
TBD

authentication/authorization
----------------------------
TBD

References
----------
- ZooKeeper REST: https://github.com/apache/zookeeper/tree/trunk/src/contrib/rest
- ephemeral parent znodes: https://issues.apache.org/jira/browse/ZOOKEEPER-723
