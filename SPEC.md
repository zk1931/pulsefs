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

Files and directories are versioned. When a new file gets created, its version
is zero. File version gets incremented when the file gets modified.

When a new directory gets created, its version is zero. Directory version gets
incremented whenever there is a change in the content of the directory.
Specifically:

- A new file gets created in the directory tree.
- A file in the directory tree gets modified.
- A file in the directory tree gets deleted.
- A new directory gets created in the directory tree.
- A directory in the directory tree gets deleted.

A special version -1 indicates that a file or a directory does not exist. This
special version is used for file/directory creation as well as watching for file
/directory creation/deletion as described later.

A special directory `/pulsed` is reserved for use by pulsed.

creating a new regular file
---------------------------

    PUT /newfile HTTP/1.1
    content-length: 13

    Hello, world!

    HTTP/1.1 200 OK
    version: 0
    content-length: 0

Note that this blindly puts the file regardless of whether it already exists or
not. To make sure the file doesn't exist, do:

    PUT /newfile2?version=-1 HTTP/1.1
    content-length: 13

    Hello, world!

    HTTP/1.1 201 Created
    version: 0
    content-length: 0

    PUT /newfile2?version=-1 HTTP/1.1
    content-length: 13

    Hello, world!

    HTTP/1.1 409 Conflict
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

Use the version parameter to do 'test and set':

    PUT /newfile?version=1 HTTP/1.1
    content-length: 21

    Hello, updated world!

    HTTP/1.1 200 OK
    version: 2
    content-length: 0

    PUT /newfile?version=1 HTTP/1.1
    content-length: 21

    Hello, updated world!

    HTTP/1.1 409 Conflict
    version: 2
    content-length: 0

deleting an existing regular file
---------------------------------

    DELETE /newfile HTTP/1.1

    HTTP/1.1 200 OK
    content-length: 0

Use the version parameter to do 'test and delete':

    PUT /file HTTP/1.1
    content-length: 13

    Hello, world!

    HTTP/1.1 201 Created
    version: 0
    content-length: 0

    DELETE /file?version=1 HTTP/1.1

    HTTP/1.1 409 Conflict
    version: 0
    content-length: 0

    DELETE /file?version=0 HTTP/1.1

    HTTP/1.1 200 OK
    content-length: 0

creating a new regular directory
--------------------------------

    PUT /newdir?dir HTTP/1.1
    content-length: 0

    HTTP/1.1 200 OK
    version: 0
    content-length: 0

creating a new regular file and all its parent directories
---------------------------

    PUT /foo/bar/newfile HTTP/1.1
    content-length: 13

    Hello, world!

    HTTP/1.1 400 Bad Request
    content-length: 45

    { "error": "Directory /foo does not exist." }

    PUT /foo/bar/newfile?recursive HTTP/1.1
    content-length: 13

    Hello, world!

    HTTP/1.1 200 OK
    version: 0
    content-length: 0

creating a new transient directory
----------------------------------

    PUT /foo/bar/file?recursive&transient HTTP/1.1
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
    location: /foo/bar/dir/0000000000000001
    version: 0
    content-length: 0

    POST /foo/bar/dir HTTP/1.1
    content-length: 6

    Second

    HTTP/1.1 200 OK
    location: /foo/bar/dir/0000000000000002
    version: 0
    content-length: 0

listing a directory
-------------------

    GET /foo/bar/dir HTTP/1.1

    HTTP/1.1 200 OK
    version: 2

    {
      "contents": [
        {
          "path": "/foo/bar/dir/0000000000000001",
          "type": "file"
          "version": 0
        },
        {
          "path": "/foo/bar/dir/0000000000000002",
          "type": "file"
          "version": 0
        }
      ]
    }

TODO: add recursive listing
TODO: add dump option to dump file contents.

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

You can also check the version of the directory before deleting:

    DELETE /newdir?recursive&version=2 HTTP/1.1

    HTTP/1.1 200 OK
    content-length: 0

waiting for a change on a file/directory
------------------------------

- Wait for a file/directory to get created. This returns immediately if the
file/directory already exists.

        GET /hello?wait=0 HTTP/1.1

- Wait until the file/directory version is at least 1. This returns immediately
if the version is already greater than or equal to 1. The server responds with
404 if the file/directory doesn't exist or it gets deleted before reaching
version 1.

        GET /hello?wait=1 HTTP/1.1

- Wait until the file/directory gets deleted. Note that the server returns 404
when the file/directory gets deleted. The server immediately responds with 404
if the file/directory does not exist.

        GET /file?wait=-1 HTTP/1.1

creating a session
------------------

    POST /pulsed/session HTTP/1.1

    HTTP/1.1 200 OK
    content-length: 0
    location: /pulsed/session/0000000000000000
    timeout-sec: 10

creating an ephemeral file
--------------------------

    PUT /newfile?ephemeral HTTP/1.1
    content-length: 13
    session: 0000000000000000

    Hello, world!

    HTTP/1.1 200 OK
    version: 0
    content-length: 0

You can also create sequential ephemeral files:

    POST /dir?ephemeral HTTP/1.1
    content-length: 13
    session: 0000000000000000

    Hello, world!

    HTTP/1.1 200 OK
    location: /dir/0000000000000000
    version: 0
    content-length: 0

sending a pulse (or a heartbeat) to a session
------------------

Send a pulse to renew a session. Pulsed expires a session if it doesn't receive
a pulse within a timeout specified in timeout-sec header in the session creation
response.

    PUT /pulsed/session/0000000000000000 HTTP/1.1
    content-length: 0

    HTTP/1.1 200 OK
    content-length: 0

TODO: pulse response could contain some useful info about the session.

deleting a session
------------------

    DELETE /pulsed/session/0000000000000000 HTTP/1.1

    HTTP/1.1 200 OK
    content-length: 0

executing multiple operations atomically
----------------------------------------
TBD

asynchronous operations
-----------------------
TBD

authentication/authorization
----------------------------
TBD

TODO: include xid (transaction id) in each response, and allow the client to
send requests with xid to ensure that the client doesn't go back in time.

References
----------
- ZooKeeper REST: https://github.com/apache/zookeeper/tree/trunk/src/contrib/rest
- ephemeral parent znodes: https://issues.apache.org/jira/browse/ZOOKEEPER-723
