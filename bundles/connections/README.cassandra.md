Setup Required for Cassandra
============================

Using cassandra-cli, create the following schema:

        connect localhost/9160;
        create keyspace SakaiOAE
          with strategy_options = {replication_factory:1}
          and placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy';
        use SakaiOAE;
        
        create column family ContactConnections
          with comparator = ‘CompositeType(UTF8Type, UTF8Type)’
          and key_validation_class = ‘UTF8Type’
          and default_validation_class = ‘UTF8Type’;
        
        create column family ContactConnections_ByState
          with comparator = ‘CompositeType(UTF8Type, UTF8Type)’
          and key_validation_class = ‘UTF8Type’
          and default_validation_class = ‘UTF8Type’;


