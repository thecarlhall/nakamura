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
 * specific language governing permissions and limitations under
 * the License.
 */
package org.sakaiproject.nakamura.connections;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.sling.commons.osgi.PropertiesUtil;
import org.osgi.service.component.ComponentException;
import org.sakaiproject.nakamura.api.connections.ConnectionException;
import org.sakaiproject.nakamura.api.connections.ConnectionState;
import org.sakaiproject.nakamura.api.connections.ConnectionStorage;
import org.sakaiproject.nakamura.api.connections.ContactConnection;
import org.sakaiproject.nakamura.api.lite.Session;
import org.sakaiproject.nakamura.api.lite.authorizable.Authorizable;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

@Component
@Service
public class AstyanaxConnectionStorage implements ConnectionStorage {
  private static final String DEFAULT_CLUSTER_NAME = "SakaiOAE";
  @Property(value = DEFAULT_CLUSTER_NAME)
  public static final String CLUSTER_NAME = "clusterName";

  private static final String DEFAULT_KEYSPACE_NAME = "SakaiOAE";
  @Property(value = DEFAULT_KEYSPACE_NAME)
  public static final String KEYSPACE_NAME = "keyspaceName";

  private static final String DEFAULT_DISCOVERY_TYPE = "NONE";
  @Property(value = DEFAULT_DISCOVERY_TYPE)
  public static final String DISCOVERY_TYPE = "discoveryType";

  @Property
  public static final String CONN_POOL_NAME = "connPoolName";

  private static final int DEFAULT_PORT = 9160;
  @Property(intValue = DEFAULT_PORT)
  public static final String PORT = "port";

  private static final int DEFAULT_MAX_CONNS_PER_HOST = 1;
  @Property(intValue = DEFAULT_MAX_CONNS_PER_HOST)
  public static final String MAX_CONNS_PER_HOST = "maxConnsPerHost";

  private static final String DEFAULT_SEEDS = "127.0.0.1:9160";
  @Property(value = DEFAULT_SEEDS)
  public static final String SEEDS = "seeds";

  private Keyspace keyspace;
//  private ColumnFamily<String, String> columnFamily;

//  static AnnotatedCompositeSerializer<ContactConnection> contactConnectionTypesSerializer = new AnnotatedCompositeSerializer<ContactConnection>(
//      ContactConnection.class);
//  static final ColumnFamily<String, String> CF_CONT_CONN_TYPES = new ColumnFamily<String, String>(
//      ContactConnection.class.getName(), // Column Family Name
//      StringSerializer.get(), // Key Serializer
//      StringSerializer.get()); // Column Serializer;

  static final ColumnFamily<String, String> CF_CONT_CONN = new ColumnFamily<String, String>(
      "contactConnections", // Column Family Name
      StringSerializer.get(), // Key Serializer
      StringSerializer.get()); // Column Serializer;

  @Activate
  @Modified
  protected void activate(Map<?, ?> props) {
    String connPoolName = PropertiesUtil.toString(props.get(CONN_POOL_NAME), null);
    if (StringUtils.isBlank(connPoolName)) {
      throw new ComponentException("'connPoolName is not set.");
    }

    String clusterName = PropertiesUtil.toString(props.get(CLUSTER_NAME), DEFAULT_CLUSTER_NAME);
    String keyspaceName = PropertiesUtil.toString(props.get(KEYSPACE_NAME), DEFAULT_KEYSPACE_NAME);
    NodeDiscoveryType discoveryType = NodeDiscoveryType.valueOf(PropertiesUtil.toString(
        props.get(DISCOVERY_TYPE), DEFAULT_DISCOVERY_TYPE));
    int port = PropertiesUtil.toInteger(props.get(PORT), DEFAULT_PORT);
    int maxConnsPerHost = PropertiesUtil.toInteger(props.get(MAX_CONNS_PER_HOST), DEFAULT_MAX_CONNS_PER_HOST);
    String seeds = PropertiesUtil.toString(props.get(SEEDS), DEFAULT_SEEDS);

    AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
        .forCluster(clusterName)
        .forKeyspace(keyspaceName)
        .withAstyanaxConfiguration(
            new AstyanaxConfigurationImpl().setDiscoveryType(discoveryType))
        .withConnectionPoolConfiguration(
            new ConnectionPoolConfigurationImpl(connPoolName).setPort(port)
                .setMaxConnsPerHost(maxConnsPerHost).setSeeds(seeds))
        .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
        .buildKeyspace(ThriftFamilyFactory.getInstance());

    context.start();
    keyspace = context.getEntity();

//    columnFamily = new ColumnFamily<String, String>(
//        ContactConnection.class.getName(), // Column Family Name
//        StringSerializer.get(), // Key Serializer
//        StringSerializer.get()); // Column Serializer;
  }

  @Override
  public ContactConnection getOrCreateContactConnection(Authorizable thisAu, Authorizable otherAu) throws ConnectionException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void saveContactConnectionPair(ContactConnection thisNode, ContactConnection otherNode) throws ConnectionException {
    MutationBatch m = keyspace.prepareMutationBatch();
    ColumnListMutation<String> mutations = m.withRow(CF_CONT_CONN, thisNode.getKey())
      .putColumn("firstName", thisNode.getFirstName(), null)
      .putColumn("fromUserId", thisNode.getFromUserId(), null)
      .putColumn("lastName", thisNode.getLastName(), null)
      .putColumn("toUserId", thisNode.getToUserId(), null)
      .putColumn("connectionState", thisNode.getConnectionState().toString(), null);

    for (Entry<String, Object> prop : thisNode.getProperties().entrySet()) {
      mutations.putColumn("properties:" + prop.getKey(), String.valueOf(prop.getValue()), null);
    }

    for (String type : thisNode.getConnectionTypes()) {
      mutations.putEmptyColumn("connectionTypes:" + type, null);
    }
//    mutations = m.withRow(CF_CONT_CONN, thisNode.getKey() + ":connectionTypes");
//    for (String type : thisNode.getConnectionTypes()) {
//      mutations.putEmptyColumn(type, null);
//    }

//    mutations = m.withRow(CF_CONT_CONN, thisNode.getKey() + ":properties");
//    for (Entry<String, Object> prop : thisNode.getProperties().entrySet()) {
//      mutations.putColumn(prop.getKey(), String.valueOf(prop.getValue()), null);
//    }
    try {
      OperationResult<Void> result = m.execute();
      Void v = result.getResult();
    } catch (com.netflix.astyanax.connectionpool.exceptions.ConnectionException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  @Override
  public ContactConnection getContactConnection(Authorizable thisUser, Authorizable otherUser) throws ConnectionException {
    ColumnFamilyQuery<String, String> query = keyspace.prepareQuery(CF_CONT_CONN);
    try {
      OperationResult<ColumnList<String>> result = query.getKey(thisUser.getId()).execute();
      ColumnList<String> columns = result.getResult();
    } catch (com.netflix.astyanax.connectionpool.exceptions.ConnectionException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    return null;
  }

  @Override
  public List<String> getConnectedUsers(Session session, String userId, ConnectionState state) throws ConnectionException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
