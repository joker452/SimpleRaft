# SimpleRaft ![](https://img.shields.io/github/license/joker452/SimpleRaft) ![](https://img.shields.io/badge/python-3.6%2B-blue)

This is a cloud-based file storage system which supports file creation, modification and deletion in a distributed manner.  

## Functionality

### Client

Multiple clients can concurrently connect to the server to access a common set of files. Clients will see consistent file states among updates.

### Server

Servers communicate with each other through RPC. In order to make different servers in the system have a consistent state, we implement leader election and log replication based on Raft.  
Info related to files on the server are composed of two parts:

1. file data: the content of each file is divided up into chunks, or blocks, each of which has a unique identifier. Server stores blocks, and when given an identifier, retrieves and returns the appropriate block.
2. metadata: each server holds the mapping of filenames to blocks.

The whole system is tested with the python framework [unittest](https://docs.python.org/3/library/unittest.html).

## How to Run

run client with

```Python
python clinet.py <host:port> <base_dir> <block size>
```

run server with

```Python
python server.py <config_file path> <server_num>
```

## Co-Author

[Xu wei](https://github.com/weixu000)
