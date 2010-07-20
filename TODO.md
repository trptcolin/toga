TODO
======

Suggestions here are welcome via GitHub message or email (trptcolin@gmail.com)

Coming Soon
=====

* API improvements:

        (def client (toga/create-client "127.0.0.1" 9160))
        (def subscribers (toga/keyspace client "Subscribers"))
        (toga/get subscribers "colin-jones")

  * this implies new data structures (Types?) and operations on them:
    * Client (keyspace)
    * Keyspace (get, put)

* Per-action ConsistencyLevel
* Gets over ColumnRanges
* Failover
* Connection pooling
* More examples / ideas for structuring data


Coming upon Cassandra 0.7 release
======

* Friendlier setup, setting up test keyspaces / column families
* Alter Keyspaces / ColumnFamilies
  - system_add_keyspace / system_drop_keyspace / system_rename_keyspace
  - system_add_column_family / system_drop_column_family / system_rename_column_family
