/*
 * Licensed to the Sakai Foundation (SF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The SF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.sakaiproject.nakamura.storage.cassandra;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.Service;
import org.apache.sling.commons.osgi.PropertiesUtil;
import org.osgi.service.component.ComponentException;
import org.sakaiproject.nakamura.api.storage.CloseableIterator;
import org.sakaiproject.nakamura.api.storage.Entity;
import org.sakaiproject.nakamura.api.storage.EntityDao;
import org.sakaiproject.nakamura.api.storage.StorageService;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 *
 */
@Component
@Service
@Properties({
    @Property(name = CassandraStorageService.CLUSTER_NAME, value = "SakaiOAE"),
    @Property(name = CassandraStorageService.KEYSPACE_NAME, value = "SakaiOAE"),
    @Property(name = CassandraStorageService.DISCOVERY_TYPE, value = "NONE"),
    @Property(name = CassandraStorageService.CONN_POOL_NAME),
    @Property(name = CassandraStorageService.PORT, intValue = 9160),
    @Property(name = CassandraStorageService.MAX_CONNS_PER_HOST, intValue = 1),
    @Property(name = CassandraStorageService.KEYSPACE_NAME, value = "127.0.0.1:9160")
})
public class CassandraStorageService implements StorageService {
  public static final String CLUSTER_NAME = "clusterName";
  public static final String KEYSPACE_NAME = "keyspaceName";
  public static final String DISCOVERY_TYPE = "discoveryType";
  public static final String CONN_POOL_NAME = "connPoolName";
  public static final String PORT = "port";
  public static final String MAX_CONNS_PER_HOST = "maxConnsPerHost";
  public static final String SEEDS = "seeds";

  private Keyspace keyspace;
  private Map<Class<?>, ColumnFamily<String, String>> columnFamilies;

  @Reference(cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE, policy = ReferencePolicy.DYNAMIC, referenceInterface = CassandraEntityDao.class)
  protected Map<String, CassandraEntityDao<?>> daos;

  @Activate
  @Modified
  public void activate(Map<?, ?> props) {
    String clusterName = PropertiesUtil.toString(props.get(CLUSTER_NAME), "SakaiOAE");
    String keyspaceName = PropertiesUtil.toString(props.get(KEYSPACE_NAME), "SakaiOAE");
    NodeDiscoveryType discoveryType = NodeDiscoveryType.valueOf(PropertiesUtil.toString(
        props.get(DISCOVERY_TYPE), NodeDiscoveryType.NONE.toString()));
    String connPoolName = PropertiesUtil.toString(props.get(CONN_POOL_NAME), null);
    int port = PropertiesUtil.toInteger(props.get(PORT), 9160);
    int maxConnsPerHost = PropertiesUtil.toInteger(props.get(MAX_CONNS_PER_HOST), 1);
    String seeds = PropertiesUtil.toString(props.get(KEYSPACE_NAME), "127.0.0.1:9160");

    if (StringUtils.isBlank(connPoolName)) {
      throw new ComponentException("'connPoolName is not set.");
    }

    // TODO don't reimpl StorageService. Make generic DAO framework
    AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
      .forCluster(clusterName)
      .forKeyspace(keyspaceName)
      .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
        .setDiscoveryType(discoveryType)
      )
      .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(connPoolName)
        .setPort(port)
        .setMaxConnsPerHost(maxConnsPerHost)
        .setSeeds(seeds)
      )
      .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
      .buildKeyspace(ThriftFamilyFactory.getInstance());

    context.start();
    keyspace = context.getEntity();
  }

  @Deactivate
  protected void deactivate() {
    keyspace = null;
  }
  
//  public CassandraStorageService(String clusterName, String hostIp, String keyspaceName,
//      String columnFamily, int replicationFactor) {
//    this.clusterName = clusterName;
//    this.hostIp = hostIp;
//    this.keyspaceName = keyspaceName;
//    this.columnFamily = columnFamily;
//    this.replicationFactor = replicationFactor;
//
//    Cluster daoCluster = HFactory.getOrCreateCluster(clusterName, hostIp);
//    KeyspaceDefinition keyspaceDef = daoCluster.describeKeyspace(keyspaceName);
//
//    // If keyspace does not exist, the CFs don't exist either. => create them.
//    if (keyspaceDef == null) {
//      ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName,
//          columnFamily, ComparatorType.BYTESTYPE);
//
//      KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(keyspaceName,
//          ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor, Arrays.asList(cfDef));
//      // Add the schema to the cluster.
//      //"true" as the second param means that Hector will block until all nodes see the change.
//      daoCluster.addKeyspace(newKeyspace, true);
//    }
//
//    keyspace = HFactory.createKeyspace(keyspaceName, daoCluster);
//    template = new ThriftColumnFamilyTemplate<String, String>(keyspace, columnFamily,
//        StringSerializer.get(), StringSerializer.get());
//
//  }

  /**
   * {@inheritDoc}
   * 
   * @see org.sakaiproject.nakamura.api.storage.StorageService#getDao(java.lang.Class)
   */
  @Override
  public <T extends Entity> EntityDao<T> getDao(Class<T> clazz) {
    CassandraEntityDao<?> dao = daos.get(clazz.getName());
    if (dao != null) {
      ColumnFamily<String, String> columnFamily = columnFamilies.get(clazz);
      if (columnFamily == null) {
        columnFamily = new ColumnFamily<String, String>(
            clazz.getName(),              // Column Family Name
            StringSerializer.get(),   // Key Serializer
            StringSerializer.get());  // Column Serializer;
        columnFamilies.put(clazz, columnFamily);
      }
      return dao.newInstance(clazz, keyspace, columnFamily);
    }
    return null;
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.sakaiproject.nakamura.api.storage.StorageService#findAll()
   */
  @Override
  public CloseableIterator<Entity> findAll() {
    return null;
  }

}
