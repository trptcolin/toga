Toga
=========

A Cassandra client wrapper for Clojure

Oh, right. You probably shouldn't use this, because afaik it's never been in production. I'd try http://clojurecassandra.info/ instead.


What is Toga?
=====

Toga allows your Clojure code to communicate with Cassandra via its
Thrift API. You can think of Toga as a wrapper for Cassandra. Get it?

It is ALPHA software. Toga has not been used in production, by anyone... Yet.  :)

While this client is intended to allow simple access to Cassandra, a working
knowledge of Cassandra is essential to getting up and running. There are a
couple of excellent articles that you should study and understand before trying
to use Toga to work with your data:

* http://wiki.apache.org/cassandra/GettingStarted
* http://arin.me/blog/wtf-is-a-supercolumn-cassandra-data-model

A more extensive list of articles is here:
* http://wiki.apache.org/cassandra/ArticlesAndPresentations


Requirements
============

* Apache Cassandra 0.6.2
  * Check the project out at git://git.apache.org/cassandra.git
  * IMPORTANT: `git checkout 0.6.2` to use the 0.6.2 version
* Leiningen (recommended to pull down the rest of the dependencies)
  * `lein deps` in the Toga directory will get you what you need

You can make sure you have everything ready to go by running your Cassandra
server on the default port (9160) and running ./script/test in the same
directory as this README.


Examples
========

For these examples, I'll assume knowledge of Cassandra terminology (Keyspace,
ColumnFamily, Column, SuperColumn).

In the first example, I'll open up a connection to a Cassandra server on the
local machine, on the default port (9160). I'll also assume that TestKeyspace is
defined in the conf/storage-conf.xml file in your Cassandra directory. If that's
not the case on your machine, you'll need to make it so to use the example. (See
the articles The result is that we insert a Column with the name "full_name" and
the value "Colin Jones", under the key "colin".

    (with-client {:host "localhost" :port 9160}
      (insert "TestKeyspace" "People" "colin" "full_name" "Colin Jones"))

While this is pretty simple, obviously we'll often want to insert multiple
related columns at once in a given ColumnFamily.  We can do that by using a
map rather than a key and value:

    (with-client {:host "localhost" :port 9160}
      (insert "TestKeyspace" "People" "colin" {"full_name" "Colin Jones"
                                               "company" "8th Light"}))

Of course, it may be inconvenient to have to repeat the Keyspace over and over
when an entire application will likely access only a single namespace. There
is an easier way that'll allow us to avoid this repetition:

    (with-client {:host "localhost" :port 9160 :keyspace "TestKeyspace"}
      (insert "People" "colin" {"full_name" "Colin Jones"
                                "company" "8th Light"}))

Alternatively, to allow switching keyspaces without re-creating a client:

    (with-client {:host "localhost" :port 9160}
      (in-keyspace "TestKeyspace"
        (insert "People" "colin" {"full_name" "Colin Jones"
                                  "company" "8th Light"})))

I think at this point we have covered insertion pretty well, right? How about
pulling things back out of the datastore:

    (with-client {:host "localhost" :port 9160 :keyspace "TestKeyspace"}
      (toga/get "People" "colin"))

Cassandra doesn't use the terminology "record" (that I'm aware of), but here we
just use it to mean a map, where columns are represented as map entries. So, given
the previous statements taken as a group, the last statement would return:

    {"full_name" "Colin Jones", "company" "8th Light"}

If we hadn't gotten any results, we'd get an empty map as the return value.

Dealing with SuperColumns is nearly as easy. Remember that a ColumnFamily holds
only columns, and a SuperColumnFamily holds only SuperColumns. The syntax is
probably what you would expect: nested maps.

    (insert "Addresses" "colin" {"mailing" {"city" "Libertyville"
                                            "state" "IL"
                                            "zip" "60048"}
                                 "email" {"domain" "8thlight.com"
                                          "user" "colin"}})

Also keep in mind that one level of nesting is all you can do: SuperColumns only
contain proper Columns, which are a string key and value in the context of Toga.



Contributing
============

This project is (obviously) still in its infancy, but contributions are
encouraged.

If you have ideas for improvements or find things that are broken,
let me know through my GitHub account: http://github.com/trptcolin or over
email (trptcolin@gmail.com) I'd love to hear thoughts on the API and how it
could be improved.

Patches are welcomed!

