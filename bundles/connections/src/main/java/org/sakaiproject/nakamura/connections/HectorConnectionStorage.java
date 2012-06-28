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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
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

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Service;
import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.sakaiproject.nakamura.api.connections.ConnectionException;
import org.sakaiproject.nakamura.api.connections.ConnectionState;
import org.sakaiproject.nakamura.api.connections.ConnectionStorage;
import org.sakaiproject.nakamura.api.connections.ContactConnection;
import org.sakaiproject.nakamura.api.lite.Session;
import org.sakaiproject.nakamura.api.lite.authorizable.Authorizable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 *
 */
@Component
@Service
public class HectorConnectionStorage implements ConnectionStorage {

  private static final String CLUSTER_NAME = "SakaiOAE";
  private static final String KEYSPACE_NAME = "SakaiOAE";
  private static final String CF_NAME = "ContactConnections";
  private static final String CF_BY_STATE_NAME = "ContactConnections_ByState";

  private Keyspace keyspace = null;
  private Map<String, ColumnFamilyTemplate<String, String>> templates = null;

  @Activate
  @Modified
  protected void activate(Map<?, ?> props) throws Exception {
    Cluster cluster = HFactory.getOrCreateCluster(CLUSTER_NAME, "localhost:9160");
    ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(KEYSPACE_NAME,
        CF_NAME, ComparatorType.BYTESTYPE);

    // TODO should make this configurable
    int replicationFactor = 1;
    KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(KEYSPACE_NAME,
        ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor, Arrays.asList(cfDef));
    // Add the schema to the cluster.
    cluster.addKeyspace(newKeyspace);
    keyspace = HFactory.createKeyspace(KEYSPACE_NAME, cluster);

    templates = Maps.newHashMap();
    templates.put(CF_NAME, new ThriftColumnFamilyTemplate<String, String>(keyspace, CF_NAME,
        StringSerializer.get(), StringSerializer.get()));

    templates.put(CF_BY_STATE_NAME, new ThriftColumnFamilyTemplate<String, String>(
        keyspace, CF_BY_STATE_NAME, StringSerializer.get(), StringSerializer.get()));
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
      cc = new ContactConnection(null, ConnectionState.NONE, Collections.<String>emptySet(),
          thisAu.getId(), otherAu.getId(), (String) otherAu.getProperty("firstName"),
          (String) otherAu.getProperty("lastName"), null);
      ColumnFamilyTemplate<String, String> template = templates.get(CF_NAME);
      if (template != null) {
        try {
          mutateContactConnection(cc, null);
        } catch (UnsupportedEncodingException e) {
          throw new ConnectionException(500, e);
        } catch (JSONException e) {
          throw new ConnectionException(500, e);
        }
      } else {
        throw new ConnectionException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
            "Unable to find connection template for " + CF_NAME);
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
    ColumnFamilyTemplate<String, String> template = templates.get(CF_NAME);
    if (template != null) {
      Mutator<String> mutator = template.createMutator();

       try {
        mutateContactConnection(thisNode, mutator);
        mutateContactConnection(otherNode, mutator);
        mutator.execute();
      } catch (HectorException e) {
        throw new ConnectionException(500, e);
      } catch (UnsupportedEncodingException e) {
        throw new ConnectionException(500, e);
      } catch (JSONException e) {
        throw new ConnectionException(500, e);
      }
    } else {
      throw new ConnectionException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Unable to find connection template for " + CF_NAME);
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
      SliceQuery<String, Composite, String> query = HFactory.createSliceQuery(keyspace,
          StringSerializer.get(),    /* key serializer */
          CompositeSerializer.get(), /* name serializer */
          StringSerializer.get()     /* value serializer */);
      query.setColumnFamily(CF_NAME);
      query.setKey(thisUser.getId());

      // get all columns that have otherUserId:* as their composite keys
      Composite start = new Composite();
      start.addComponent(otherUser.getId(), StringSerializer.get())
          .addComponent(Character.toString(Character.MIN_VALUE), StringSerializer.get());

      Composite finish = new Composite();
      finish.addComponent(otherUser.getId(), StringSerializer.get())
          .addComponent(Character.toString(Character.MAX_VALUE), StringSerializer.get());
      query.setRange(start, finish, false, 0);

      ColumnSlice<Composite, String> queryResult = query.execute().get();
      List<HColumn<Composite, String>> columns = queryResult.getColumns();

      ContactConnection cc = null;
      if (!columns.isEmpty()) {
        ConnectionState state = null;
        String fromUserId = null;
        String toUserId = null;
        String firstName = null;
        String lastName = null;
        Set<String> types = Sets.newHashSet();
        Map<String, Object> props = Maps.newHashMap();
        for (HColumn<Composite,String> column : columns) {
          String propertyName = column.getName().getComponent(1).toString();
          if ("connectionState".equals(propertyName)) {
            state = ConnectionState.valueOf(column.getValue());
          } else if ("firstName".equals(propertyName)) {
            firstName = column.getValue();
          } else if ("lastName".equals(propertyName)) {
            lastName = column.getValue();
          } else if ("types".equals(propertyName)) {
            JSONArray jsonTypes = new JSONArray(column.getValue());
            for (int i = 0; i < jsonTypes.length(); i++) {
              String type = jsonTypes.getString(i);
              types.add(type);
            }
          } else if ("properties".equals(propertyName)) {
            JSONObject jsonProps = new JSONObject(column.getValue());
            for (Iterator<String> keys = jsonProps.keys(); keys.hasNext(); ) {
              String key = keys.next();
              props.put(key, jsonProps.get(key));
            }
          }
        }

        cc = new ContactConnection(fromUserId, state, types, fromUserId, toUserId, firstName, lastName, props);
      }
      return cc;
    } catch (HectorException e) {
      throw new ConnectionException(500, e);
    } catch (JSONException e) {
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
    SliceQuery<String, Composite, String> query = HFactory.createSliceQuery(keyspace,
        StringSerializer.get(), CompositeSerializer.get(), StringSerializer.get());
    query.setColumnFamily(CF_BY_STATE_NAME);
    query.setKey(userId);

    Composite start = new Composite();
    start.addComponent(state.toString(), StringSerializer.get())
        .addComponent(Character.toString(Character.MIN_VALUE), StringSerializer.get());
    Composite finish = new Composite();
    finish.addComponent(state.toString(), StringSerializer.get())
        .addComponent(Character.toString(Character.MAX_VALUE), StringSerializer.get());
    query.setRange(start, finish, false, 0);

    List<String> connections = Lists.newArrayList();
    ColumnSlice<Composite, String> queryResult = query.execute().get();
    List<HColumn<Composite, String>> columns = queryResult.getColumns();
    String connState = state.toString();
    for (HColumn<Composite,String> column : columns) {
      if (connState.equals(column.getValue())) {
        connections.add(column.getName().getComponent(1).toString());
      }
    }
    return connections;
  }

  // --------------- Helper methods ---------------
  /**
   * @param connection
   * @param key
   * @param mutator
   * @throws UnsupportedEncodingException 
   */
  private void mutateContactConnection(ContactConnection connection,
      Mutator<String> mutator) throws UnsupportedEncodingException, JSONException {
    Mutator<String> _mutator = mutator;
    if (_mutator == null) {
      ColumnFamilyTemplate<String, String> template = templates.get(CF_NAME);
      if (template != null) {
        _mutator = template.createMutator();
      } else {
        throw new ConnectionException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
            "Unable to find connection template for " + CF_NAME);
      }
    }
    ColumnFamilyTemplate<String, String> templateByState = templates.get(CF_BY_STATE_NAME);
    if (templateByState == null) {
      throw new ConnectionException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Unable to find connection template for " + CF_NAME);
    }

    // persist the connection by fromUser: {toUser, propName} = propVal
    String fromUser = connection.getFromUserId();
    String toUser = connection.getToUserId();
    String connectionState = connection.getConnectionState().toString();

    addInsertion(_mutator, fromUser, CF_NAME, toUser, "firstName", connection.getFirstName());
    addInsertion(_mutator, fromUser, CF_NAME, toUser, "lastName", connection.getLastName());
    addInsertion(_mutator, fromUser, CF_NAME, toUser, "fromUserId", connection.getFromUserId());
    addInsertion(_mutator, fromUser, CF_NAME, toUser, "toUserId", connection.getToUserId());
    addInsertion(_mutator, fromUser, CF_NAME, toUser, "connectionState", connectionState);

    // Serialize types to a JSON string since we only get them back as a whole and don't search into them
    JSONArray types = new JSONArray();
    for (String type : connection.getConnectionTypes()) {
      types.put(type);
    }
    addInsertion(_mutator, fromUser, CF_NAME, toUser, "types", types.toString());

    // Serialize properties to a JSON string since we only get them back as a whole and don't search into them
    JSONObject props = new JSONObject();
    for (Entry<String, Object> property : connection.getProperties().entrySet()) {
      props.put(property.getKey(), property.getValue());
    }
    addInsertion(_mutator, fromUser, CF_NAME, toUser, "properties", props.toString());

    if (mutator == null) {
      _mutator.execute();
    }

    // persist the connection by status. this allows us to get all users connected with a
    // certain state in a single column slice.
    Mutator<String> stateMutator = templateByState.createMutator();
    addInsertion(stateMutator, fromUser, CF_BY_STATE_NAME, connectionState, toUser, null);
    stateMutator.execute();
  }

  private void addInsertion(Mutator<String> mutator, String key, String columnFamilyName,
      String columnKey1, String columnKey2, String value)
      throws UnsupportedEncodingException {
    byte[] bytes = (value != null) ? value.getBytes("UTF-8") : null;
    HColumn<Composite, byte[]> column = createCompositeColumn(columnKey1, columnKey2, bytes);
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
    HColumn<Composite, byte[]> column = HFactory.createColumn(composite, (value != null) ? value : new byte[0], CompositeSerializer.get(), BytesArraySerializer.get());
    return column;
  }
}
