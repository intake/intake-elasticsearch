Quickstart
==========

``intake-elasticsearch`` provides quick and easy access to tabular data stored in
`ElasticSearch`_

.. _ElasticSearch: https://www.elastic.co/

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
 will become available.

Creating Catalog Entries
~~~~~~~~~~~~~~~~~~~~~~~~

To include in a catalog, the plugin must be listed in the plugins of the catalog::

   plugins:
     source:
       - module: intake_elasticsearch

and entries must specify ``driver: elasticsearch_table``.

Using a Catalog
~~~~~~~~~~~~~~~

