/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sakaiproject.nakamura.api.connections.ConnectionConstants;
import org.sakaiproject.nakamura.api.connections.ConnectionException;
import org.sakaiproject.nakamura.api.connections.ConnectionState;
import org.sakaiproject.nakamura.api.connections.ContactConnection;
import org.sakaiproject.nakamura.api.lite.Session;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AclModification;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AclModification.Operation;
import org.sakaiproject.nakamura.api.lite.accesscontrol.Permissions;
import org.sakaiproject.nakamura.api.lite.accesscontrol.Security;
import org.sakaiproject.nakamura.api.lite.authorizable.Authorizable;
import org.sakaiproject.nakamura.api.user.UserConstants;
import org.sakaiproject.nakamura.lite.BaseMemoryRepository;
import org.sakaiproject.nakamura.lite.RepositoryImpl;
import org.sakaiproject.nakamura.util.ExtendedJSONWriter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 *
 */
public class ConnectionManagerImplTest {

  private ConnectionManagerImpl connectionManager;
  private RepositoryImpl repository;
  private SparseMapConnectionStorage connectionStorage;

  @Before
  public void setUp() throws StorageClientException, AccessDeniedException, ClassNotFoundException, IOException {
    BaseMemoryRepository baseMemoryRepository = new BaseMemoryRepository();
    repository = baseMemoryRepository.getRepository();
    connectionManager = new ConnectionManagerImpl();
    connectionStorage = new SparseMapConnectionStorage();
    connectionStorage.repository = repository;
    connectionManager.connectionStorage = connectionStorage;
  }

  @Test
  public void testAddArbitraryProperties() throws ConnectionException, StorageClientException, AccessDeniedException {
    Session session = repository.loginAdministrative();
    session.getAuthorizableManager().createUser("bob", "bob", "test", null);
    session.getAuthorizableManager().createUser("alice", "alice", "test", null);
    
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put("alfa", new String[] { "a" });
    properties.put("beta", new String[] { "a", "b" });
    properties.put("charlie", "c");
    Authorizable alice = session.getAuthorizableManager().findAuthorizable("alice");
    Authorizable bob = session.getAuthorizableManager().findAuthorizable("bob");
    ContactConnection contactConnectionA = new ContactConnection(null, null, null, "alice", "bob", "Bob", "Barker", properties);
    ContactConnection contactConnectionB = new ContactConnection(null, null, null, "bob", "alice", "Alice", "Annie", null);
    connectionStorage.saveContactConnectionPair(contactConnectionA, contactConnectionB);
    ContactConnection connectionForAliceAndBob = connectionStorage.getContactConnection(alice, bob);
    Assert.assertArrayEquals((String[])connectionForAliceAndBob.getProperty("alfa"), new String[]{"a"});
    Assert.assertArrayEquals((String[])connectionForAliceAndBob.getProperty("beta"), new String[]{"a","b"});
    assertEquals(connectionForAliceAndBob.getProperty("charlie"), "c");
  }

  @Test
  public void testHandleInvitation() throws ConnectionException, StorageClientException, AccessDeniedException  {
    Session session = repository.loginAdministrative();
    session.getAuthorizableManager().createUser("bob", "bob", "test", null);
    session.getAuthorizableManager().createUser("alice", "alice", "test", null);
    Authorizable alice = session.getAuthorizableManager().findAuthorizable("alice");
    Authorizable bob = session.getAuthorizableManager().findAuthorizable("bob");

    ContactConnection fromConnection = connectionStorage.getOrCreateContactConnection(alice, bob);
    ContactConnection toConnection = connectionStorage.getOrCreateContactConnection(bob, alice);

    Map<String, String[]> props = new HashMap<String, String[]>();

    props.put(ConnectionConstants.PARAM_FROM_RELATIONSHIPS, new String[] { "Supervisor",
        "Lecturer" });
    props.put(ConnectionConstants.PARAM_TO_RELATIONSHIPS, new String[] { "Supervised",
        "Student" });
    props.put(ConnectionConstants.SAKAI_CONNECTION_TYPES, new String[] { "foo" });
    props.put("random", new String[] { "israndom" });

    connectionManager.handleInvitation(props, fromConnection, toConnection);

    Set<String> fromValues = fromConnection.getConnectionTypes();

    Set<String> toValues = toConnection.getConnectionTypes();

    assertEquals(3, fromValues.size());
    int j = 0;
    // order may not be what we expect it to be
    for(String connectionType : fromValues) {
      if ( "foo".equals(connectionType)) {
        j = j|1;
      }
      if ( "Lecturer".equals(connectionType)) {
        j = j|2;
      }
      if ( "Supervisor".equals(connectionType)) {
        j = j|4;
      }
    }

    Assert.assertTrue((j&1)==1);
    Assert.assertTrue((j&2)==2);
    Assert.assertTrue((j&4)==4);
    assertEquals(3, toValues.size());

    j = 0;
    for (String connectionType : toValues) {
      if ( "foo".equals(connectionType)) {
        j = j|1;
      }
      if ( "Student".equals(connectionType)) {
        j = j|2;
      }
      if ( "Supervised".equals(connectionType)) {
        j = j|4;
      }
    }
    Assert.assertTrue((j&1)==1);
    Assert.assertTrue((j&2)==2);
    Assert.assertTrue((j&4)==4);


    String fromRandomValues = (String) fromConnection.getProperty("random");
    String toRandomValues =  (String) toConnection.getProperty("random");

    assertEquals("israndom", fromRandomValues);
    assertEquals("israndom", toRandomValues);
  }

  @Test
  public void testCheckValidUserIdAnon() {
    Session session = mock(Session.class);
    try {
      connectionManager.checkValidUserId(session, UserConstants.ANON_USERID);
      fail("This should've thrown a ConnectionException.");
    } catch (ConnectionException e) {
      assertEquals(403, e.getCode());
    }
  }

  @Test
  public void testCheckValidUserIdNonExisting() throws AccessDeniedException, StorageClientException  {
    Session session = repository.loginAdministrative();
    session.getAuthorizableManager().createUser("bob", "bob", "test", null);
    session.logout();
    session = repository.loginAdministrative("bob");
    
    try {
      connectionManager.checkValidUserId(session, "alice");
      fail("This should've thrown a ConnectionException.");
    } catch (ConnectionException e) {
      assertEquals(404, e.getCode());
    }
  }

  @Test
  public void testCheckValidUserId() throws ConnectionException, StorageClientException, AccessDeniedException {
    Session session = repository.loginAdministrative();
    session.getAuthorizableManager().createUser("bob", "bob", "test", null);
    session.getAuthorizableManager().createUser("alice", "alice", "test", null);
    session.logout();
    session = repository.loginAdministrative("bob");
    Authorizable actual = connectionManager.checkValidUserId(session, "alice");
    assertEquals("alice", actual.getId());
  }

  @Test
  public void testGetConnectionState() throws ConnectionException, StorageClientException, AccessDeniedException {
    // Passing in null
    try {
      ContactConnection nullContactConnection = new ContactConnection(null, null, null, null, null, null, null, null);
      final ConnectionState state = nullContactConnection.getConnectionState();
      assertEquals("Passing in null should return ConnectionState.NONE",
          ConnectionState.NONE, state);
    } catch (Exception e) {
      fail("Passing in null should return ConnectionState.NONE.");
    }
  }

  @Test
  public void testDeepGetCreateNodeExisting() throws StorageClientException, AccessDeniedException, ConnectionException {
    Session session = repository.loginAdministrative();
    session.getAuthorizableManager().createUser("bob", "bob", "test", null);
    session.getAuthorizableManager().createUser("alice", "alice", "test", null);
    session.getAccessControlManager().setAcl(Security.ZONE_CONTENT, "a:alice",new AclModification[]{
        new AclModification(AclModification.grantKey("alice"), Permissions.CAN_MANAGE.getPermission(), Operation.OP_REPLACE)
    });
    session.getAccessControlManager().setAcl(Security.ZONE_CONTENT, "a:bob",new AclModification[]{
        new AclModification(AclModification.grantKey("bob"), Permissions.CAN_MANAGE.getPermission(), Operation.OP_REPLACE)
    });
    session.logout();
    session = repository.loginAdministrative("bob");
    Authorizable from = session.getAuthorizableManager().findAuthorizable("bob");
    Authorizable to = session.getAuthorizableManager().findAuthorizable("alice");
    
    ContactConnection result = connectionStorage.getOrCreateContactConnection(from, to);
    assertEquals("bob", result.getFromUserId());
    assertEquals("alice", result.getToUserId());
  }

  @Test
  public void testDeepGetCreateNodeExistingBase() throws AccessDeniedException, StorageClientException, ConnectionException {
    Session session = repository.loginAdministrative();
    session.getAuthorizableManager().createUser("bob", "bob", "test", null);
    session.getAuthorizableManager().createUser("alice", "alice", "test", null);
    session.getAccessControlManager().setAcl(Security.ZONE_CONTENT, "a:alice",new AclModification[]{
        new AclModification(AclModification.grantKey("alice"), Permissions.CAN_MANAGE.getPermission(), Operation.OP_REPLACE)
    });
    session.getAccessControlManager().setAcl(Security.ZONE_CONTENT, "a:bob",new AclModification[]{
        new AclModification(AclModification.grantKey("bob"), Permissions.CAN_MANAGE.getPermission(), Operation.OP_REPLACE)
    });
    session.logout();
    session = repository.loginAdministrative("bob");
    Authorizable from = session.getAuthorizableManager().findAuthorizable("bob");
    Authorizable to = session.getAuthorizableManager().findAuthorizable("alice");


    ContactConnection result = connectionStorage.getOrCreateContactConnection(from, to);
    assertEquals("bob", result.getFromUserId());
    assertEquals("alice", result.getToUserId());
    assertEquals("a:alice/public/authprofile", result.getProperty("reference"));
  }

  @Test
  public void testCanWriteDetailsForNonexistentConnection() throws Exception {
    Session session = repository.loginAdministrative();
    session.getAuthorizableManager().createUser("bob", "bob", "test", null);
    session.getAuthorizableManager().createUser("alice", "alice", "test", null);
    // note that alice and bob are NOT connected.
    StringWriter output = new StringWriter();
    connectionManager.writeConnectionInfo(new ExtendedJSONWriter(output), session, "bob", "alice");
    assertEquals("", output.toString());
  }
}
