Quickstart
==========

``intake-elasticsearch`` provides quick and easy access to tabular data stored in
`ElasticSearch`_

.. _ElasticSearch: https://www.elastic.co/

This plugin reads ElasticSearch query results without random access: there is only ever
a single partition.

Installation
------------

To use this plugin for `intake`_, install with the following command::

   conda install -c intake intake-elasticsearch

.. _intake: https://github.com/ContinuumIO/intake

Usage
-----

Ad-hoc
~~~~~~

After installation, the function ``intake.open_elasticsearch_table``
will become available. It can be used to execute queries on the ElasticSearch
server, and download the results as a data-frame.

Three parameters are of interest when defining a data source:

- query: the query to execute, which can be defined either using `Lucene`_ or
  `JSON`_ syntax, both of which are to be provided as a string.

- qargs: further `arguments`_ to pass along with the query, such as the index(es)
  to consider, sorting and any filters to apply

- other arguments are passed as parameters to the server `connection`_ instance,

.. _Lucene: https://www.elastic.co/guide/en/kibana/current/lucene-query.html

.. _JSON: https://www.elastic.co/guide/en/elasticsearch/reference/1.4/_introducing_the_query_language.html

.. _arguments: https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.search

.. _connection: https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch

In the simplest case, this might look something like::

   import intake
   source = intake.open_elasticsearch_table("*:*", host='elastic.server', port=9200,
       qargs={'index': 'mydocuments'})
   result = source.read()

Where ``"*:*"`` is Lucene syntax for "match all", so this will grab every document
within the given index, as a data-frame. The host and port parameters define the connection
to the ElasticSearch server.

Further parameters which can be used to modify how the source works are as follows. These
are likely not altered often.

- scroll: a text string specifying how long the query remains live on the server

- size: the number of entries to download in a single call; smaller numers will download
  slower, but may be more stable.

Creating Catalog Entries
~~~~~~~~~~~~~~~~~~~~~~~~

To include in a catalog, the plugin must be listed in the plugins of the catalog::

   plugins:
     source:
       - module: intake_elasticsearch

and entries must specify ``driver: elasticsearch_table``.

Aside from this, the same arguments are available as for ad-hoc usage. Note that queries
are commonly multi-line, especially is using JSON syntax, so the YAML ``"|"`` characted
should be used to define them within the catalog file. A full entry may look something like::

    args:
      qargs:
        index: intake_test
        doc_type: entry
      query: |
          {
          "query": {
              "match":
                  {"typeid": 1}
              },
          "sort": {
              "price": {"order": "desc"}
              },
          "_source": ["price", "typeid"]
          }
      host: intake_es


where we have specified both the index and document types (these could have been lists), the fields
to extract and sort order, as well as a matching term, loosely equivalent to ``"WHERE typeid = 1"``
in SQL.

Using a Catalog
~~~~~~~~~~~~~~~

Assuming a catalog file ``'cat.yaml'``, and an entry called ``'es_data'``, the corresponding
dataframe could be fetched as follows::

   import intake
   cat = intake.Catalog('cat.yaml')
   result = cat.es_data.read()

Since the query cannot be partitioned with this plugin, the other methods of the data source
(iterate, read one partition, create Dask data-frame) are not particularly useful here.