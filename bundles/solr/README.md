Solr Bundle for OSGi
====================

This bundle offers Solr functionality in embedded form to an OSGi Container. It listens to the OSGi container for events indicating that a resource has been updated and performs an indexing operation based on that update.

It also provides a search interface and an optional interface to a remote Solr server.

Configuration files
===================

The configuration files in `src/main/resources/` are adapted from the Apache Solr `examples` source code.

The `master-solrconfig.xml` and `slave-solrconfig.xml` files can be used as starting points for replication-style Solr clustering. This type of Solr cluster contfiguration is documented at:
http://wiki.apache.org/solr/SolrReplication

Alternatively, a SolrCloud type of cluster configuration is documented at:
http://wiki.apache.org/solr/SolrCloud
