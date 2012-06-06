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
package org.sakaiproject.nakamura.connections;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.commons.lang.StringUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Service;
import org.sakaiproject.nakamura.api.connections.ConnectionException;
import org.sakaiproject.nakamura.api.connections.ConnectionState;
import org.sakaiproject.nakamura.api.connections.ConnectionStorage;
import org.sakaiproject.nakamura.api.connections.ContactConnection;
import org.sakaiproject.nakamura.api.lite.Session;
import org.sakaiproject.nakamura.api.lite.authorizable.Authorizable;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import edu.emory.mathcs.backport.java.util.Collections;

/**
 *
 */
@Component
@Service
public class HectorConnectionStorage implements ConnectionStorage {

  private static final String CLUSTER_NAME = "SakaiOAE";
  private static final String KEYSPACE_NAME = "SakaiOAE";
  private static final String CF_NAME = "contactConnections";

  private Keyspace keyspace = null;
  private ColumnFamilyTemplate<String, String> template = null;

  @Activate
  @Modified
  protected void activate(Map<?, ?> props) throws Exception {
    Cluster cluster = HFactory.getOrCreateCluster(CLUSTER_NAME, "localhost:9160");
    ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(KEYSPACE_NAME,
        CF_NAME, ComparatorType.BYTESTYPE);

    int replicationFactor = 1;
    KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(KEYSPACE_NAME,
        ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor, Arrays.asList(cfDef));
    // Add the schema to the cluster.
    cluster.addKeyspace(newKeyspace);
    keyspace = HFactory.createKeyspace(KEYSPACE_NAME, cluster);

    template = new ThriftColumnFamilyTemplate<String, String>(keyspace, CF_NAME,
        StringSerializer.get(), StringSerializer.get());
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.sakaiproject.nakamura.api.connections.ConnectionStorage#getOrCreateContactConnection(org.sakaiproject.nakamura.api.lite.authorizable.Authorizable,
   *      org.sakaiproject.nakamura.api.lite.authorizable.Authorizable)
   */
  @Override
  public ContactConnection getOrCreateContactConnection(Authorizable thisAu,
      Authorizable otherAu) throws ConnectionException {
    ContactConnection cc = getContactConnection(thisAu, otherAu);
    if (cc == null) {
      cc = new ContactConnection(null, ConnectionState.NONE, Collections.emptySet(),
          thisAu.getId(), otherAu.getId(), (String) otherAu.getProperty("firstName"),
          (String) otherAu.getProperty("lastName"), null);
      Mutator<String> mutator = template.createMutator();
      try {
        mutateContactConnection(cc, mutator);
        mutator.execute();
      } catch (UnsupportedEncodingException e) {
        throw new ConnectionException(500, e);
      }
    }
    return cc;
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.sakaiproject.nakamura.api.connections.ConnectionStorage#saveContactConnectionPair(org.sakaiproject.nakamura.api.connections.ContactConnection,
   *      org.sakaiproject.nakamura.api.connections.ContactConnection)
   */
  @Override
  public void saveContactConnectionPair(ContactConnection thisNode,
      ContactConnection otherNode) throws ConnectionException {
    Mutator<String> mutator = template.createMutator();

     try {
      mutateContactConnection(thisNode, mutator);
      mutateContactConnection(otherNode, mutator);
      mutator.execute();
    } catch (HectorException e) {
      throw new ConnectionException(500, e);
    } catch (UnsupportedEncodingException e) {
      throw new ConnectionException(500, e);
    }
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.sakaiproject.nakamura.api.connections.ConnectionStorage#getContactConnection(org.sakaiproject.nakamura.api.lite.authorizable.Authorizable,
   *      org.sakaiproject.nakamura.api.lite.authorizable.Authorizable)
   */
  @Override
  public ContactConnection getContactConnection(Authorizable thisUser,
      Authorizable otherUser) throws ConnectionException {
    try {
      Composite start = new Composite();
      start.addComponent(otherUser.getId(), StringSerializer.get())
          .addComponent(Character.toString(Character.MIN_VALUE), StringSerializer.get());
      Composite finish = new Composite();
      finish.addComponent(otherUser.getId(), StringSerializer.get())
          .addComponent(Character.toString(Character.MAX_VALUE), StringSerializer.get());

      SliceQuery<String, Composite, String> query = HFactory.createSliceQuery(keyspace, StringSerializer.get(), CompositeSerializer.get(), StringSerializer.get());
      query.setColumnFamily(CF_NAME);
      query.setKey(thisUser.getId());
      query.setRange(start, finish, false, 0);
      ColumnSlice<Composite, String> queryResult = query.execute().get();
      List<HColumn<Composite, String>> columns = queryResult.getColumns();
      
      // TODO use columns from above
//      ColumnFamilyResult<String, String> res = template.queryColumns(thisUser.getId());
      ContactConnection cc = null;
      if (!columns.isEmpty()) {
//        String key = res.getKey();
        ConnectionState state = null;
        String fromUserId = null;
        String toUserId = null;
        String firstName = null;
        String lastName = null;
        Set<String> types = Sets.newHashSet();
        Map<String, Object> props = Maps.newHashMap();
        for (HColumn<Composite,String> column : columns) {
          String propertyName = (String) column.getName().getComponent(1).getValue();
//        }
          if ("connectionState".equals(propertyName)) {
            state = ConnectionState.valueOf(column.getValue());
          } else if ("firstName".equals(propertyName)) {
            firstName = column.getValue();
          } else if ("lastName".equals(propertyName)) {
            lastName = column.getValue();
          } else if (propertyName.startsWith("connectionTypes:")) {
            types.add(StringUtils.split(propertyName, ":", 2)[1]);
          } else if (propertyName.startsWith("properties:")) {
            String propName = StringUtils.split(propertyName, ":", 2)[1];
            props.put(propName, column.getValue());
          }
        }

        cc = new ContactConnection(key, state, types, fromUserId, toUserId, firstName, lastName, props);
      }
      return cc;
    } catch (HectorException e) {
      throw new ConnectionException(500, e);
    }
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.sakaiproject.nakamura.api.connections.ConnectionStorage#getConnectedUsers(org.sakaiproject.nakamura.api.lite.Session,
   *      java.lang.String, org.sakaiproject.nakamura.api.connections.ConnectionState)
   */
  @Override
  public List<String> getConnectedUsers(Session session, String userId,
      ConnectionState state) throws ConnectionException {
    SliceQuery<String, Composite, String> query = HFactory.createSliceQuery(keyspace, StringSerializer.get(), CompositeSerializer.get(), StringSerializer.get());
    query.setColumnFamily(CF_NAME);
    query.setKey(userId);

    Composite start = new Composite();
    start.addComponent(Character.toString(Character.MIN_VALUE), StringSerializer.get())
        .addComponent("connectionState", StringSerializer.get());
    Composite finish = new Composite();
    finish.addComponent(Character.toString(Character.MAX_VALUE), StringSerializer.get())
        .addComponent("connectionState", StringSerializer.get());
    query.setRange(start, finish, false, 0);

    ColumnSlice<Composite, String> queryResult = query.execute().get();
    List<HColumn<Composite, String>> columns = queryResult.getColumns();
//    ColumnFamilyResult<String, String> res = template.queryColumns(userId);
    ContactConnection cc = null;
    if (!columns.isEmpty()) {
      
    }
    return null;
  }

  // --------------- Helper methods ---------------
  /**
   * @param connection
   * @param key
   * @param mutator
   * @throws UnsupportedEncodingException 
   */
  private void mutateContactConnection(ContactConnection connection, Mutator<String> mutator) throws UnsupportedEncodingException {
    String fromUser = connection.getFromUserId();
    String toUser = connection.getToUserId();
    
    addInsertion(mutator, fromUser, CF_NAME, toUser, "firstName", connection.getFirstName());
    addInsertion(mutator, fromUser, CF_NAME, toUser, "lastName", connection.getFirstName());
    addInsertion(mutator, fromUser, CF_NAME, toUser, "fromUserId", connection.getFromUserId());
    addInsertion(mutator, fromUser, CF_NAME, toUser, "lastName", connection.getLastName());
    addInsertion(mutator, fromUser, CF_NAME, toUser, "toUserId", connection.getToUserId());
    addInsertion(mutator, fromUser, CF_NAME, toUser, "connectionState", connection.getConnectionState().toString());

    for (Entry<String, Object> prop : connection.getProperties().entrySet()) {
      mutator.addInsertion(fromUser, CF_NAME, HFactory.createColumn("properties:" + prop.getKey(), prop.getValue()));
    }

    byte[] emptyValue = new byte[0];
    for (String type : connection.getConnectionTypes()) {
      mutator.addInsertion(fromUser, CF_NAME, HFactory.createColumn("connectionTypes:" + type, emptyValue));
    }
  }

  private void addInsertion(Mutator<String> mutator, String key, String columnFamilyName,
      String columnKey1, String columnKey2, String value)
      throws UnsupportedEncodingException {
    HColumn<Composite, byte[]> column = createCompositeColumn(columnKey1, columnKey2, value.getBytes("UTF-8"));
    mutator.addInsertion(key, columnFamilyName, column);
  }

  /**
   * Convenience method to create a composite column of <code>{key1, key2} = value</code>.
   *
   * @param key1
   * @param key2
   * @param value
   * @return
   */
  private HColumn<Composite, byte[]> createCompositeColumn(String key1, String key2, byte[] value) {
    Composite composite = new Composite();
    composite.addComponent(key1, StringSerializer.get());
    composite.addComponent(key2, StringSerializer.get());
    HColumn<Composite, byte[]> column = HFactory.createColumn(composite, value, CompositeSerializer.get(), BytesArraySerializer.get());
    return column;
  }
}
