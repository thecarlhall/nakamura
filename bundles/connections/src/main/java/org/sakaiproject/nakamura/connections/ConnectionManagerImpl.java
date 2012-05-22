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

import static org.sakaiproject.nakamura.api.connections.ConnectionConstants.SAKAI_CONNECTION_STATE;
import static org.sakaiproject.nakamura.api.connections.ConnectionConstants.SAKAI_CONNECTION_TYPES;
import static org.sakaiproject.nakamura.api.connections.ConnectionOperation.accept;
import static org.sakaiproject.nakamura.api.connections.ConnectionOperation.block;
import static org.sakaiproject.nakamura.api.connections.ConnectionOperation.cancel;
import static org.sakaiproject.nakamura.api.connections.ConnectionOperation.ignore;
import static org.sakaiproject.nakamura.api.connections.ConnectionOperation.invite;
import static org.sakaiproject.nakamura.api.connections.ConnectionOperation.reject;
import static org.sakaiproject.nakamura.api.connections.ConnectionOperation.remove;
import static org.sakaiproject.nakamura.api.connections.ConnectionState.ACCEPTED;
import static org.sakaiproject.nakamura.api.connections.ConnectionState.BLOCKED;
import static org.sakaiproject.nakamura.api.connections.ConnectionState.IGNORED;
import static org.sakaiproject.nakamura.api.connections.ConnectionState.INVITED;
import static org.sakaiproject.nakamura.api.connections.ConnectionState.NONE;
import static org.sakaiproject.nakamura.api.connections.ConnectionState.PENDING;
import static org.sakaiproject.nakamura.api.connections.ConnectionState.REJECTED;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.sling.commons.json.JSONException;
import org.sakaiproject.nakamura.api.connections.ConnectionConstants;
import org.sakaiproject.nakamura.api.connections.ConnectionException;
import org.sakaiproject.nakamura.api.connections.ConnectionManager;
import org.sakaiproject.nakamura.api.connections.ConnectionOperation;
import org.sakaiproject.nakamura.api.connections.ConnectionState;
import org.sakaiproject.nakamura.api.connections.ConnectionStorage;
import org.sakaiproject.nakamura.api.connections.ContactConnection;
import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.Repository;
import org.sakaiproject.nakamura.api.lite.Session;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessControlManager;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AclModification;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AclModification.Operation;
import org.sakaiproject.nakamura.api.lite.accesscontrol.Permissions;
import org.sakaiproject.nakamura.api.lite.accesscontrol.Security;
import org.sakaiproject.nakamura.api.lite.authorizable.Authorizable;
import org.sakaiproject.nakamura.api.lite.authorizable.AuthorizableManager;
import org.sakaiproject.nakamura.api.lite.authorizable.Group;
import org.sakaiproject.nakamura.api.lite.authorizable.User;
import org.sakaiproject.nakamura.util.ExtendedJSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;




/**
 * Service for doing operations with connections.
 */
@Component(immediate = true, description = "Service for doing operations with connections.", label = "ConnectionSearchResultProcessor")
@Properties(value = { @Property(name = "service.vendor", value = "The Sakai Foundation") })
@Service(value = ConnectionManager.class)
public class ConnectionManagerImpl implements ConnectionManager {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(ConnectionManagerImpl.class);


  @Reference
  protected transient Repository repository;

  @Reference
  protected transient ConnectionStorage connectionStorage;


  private static Map<TransitionKey, StatePair> stateMap = new HashMap<TransitionKey, StatePair>();

  static {
    stateMap.put(tk(NONE, NONE, invite), sp(PENDING, INVITED)); // t1
    stateMap.put(tk(REJECTED, REJECTED, invite), sp(PENDING, INVITED)); // t2
    stateMap.put(tk(PENDING, IGNORED, invite), sp(PENDING, INVITED)); // t3
    stateMap.put(tk(PENDING, INVITED, cancel), sp(NONE, NONE)); // t4
    stateMap.put(tk(PENDING, IGNORED, cancel), sp(NONE, NONE)); // t5
    stateMap.put(tk(PENDING, BLOCKED, cancel), sp(NONE, BLOCKED)); // t6
    stateMap.put(tk(INVITED, PENDING, accept), sp(ACCEPTED, ACCEPTED)); // t7
    stateMap.put(tk(INVITED, PENDING, reject), sp(REJECTED, REJECTED)); // t8
    stateMap.put(tk(INVITED, PENDING, ignore), sp(IGNORED, PENDING)); // t9
    stateMap.put(tk(INVITED, PENDING, block), sp(BLOCKED, PENDING)); // t10
    stateMap.put(tk(ACCEPTED, ACCEPTED, remove), sp(NONE, NONE)); // t11
    stateMap.put(tk(REJECTED, REJECTED, remove), sp(NONE, NONE)); // t12
    stateMap.put(tk(IGNORED, PENDING, remove), sp(NONE, NONE)); // t13
    stateMap.put(tk(PENDING, IGNORED, remove), sp(NONE, NONE)); // t14
    stateMap.put(tk(BLOCKED, PENDING, remove), sp(NONE, NONE)); // t15
    stateMap.put(tk(PENDING, BLOCKED, remove), sp(NONE, BLOCKED)); // t16
    stateMap.put(tk(NONE, BLOCKED, invite), sp(PENDING, BLOCKED)); // t17
    stateMap.put(tk(IGNORED, PENDING, invite), sp(PENDING, INVITED)); // t19
    stateMap.put(tk(INVITED, PENDING, invite), sp(ACCEPTED, ACCEPTED)); // t20
    stateMap.put(tk(NONE, NONE, remove), sp(NONE, NONE)); // t21
    stateMap.put(tk(NONE, BLOCKED, remove), sp(NONE, BLOCKED)); // t22
    stateMap.put(tk(BLOCKED, NONE, remove), sp(NONE, NONE)); // t23
  }

  /**
   * @param thisState the first ConnectionState of the StatePair
   * @param otherState the second ConnectionState of the StatePair
   * @return a StatePair object consisting of the two provided ConnectionStates
   */
  private static StatePair sp(ConnectionState thisState, ConnectionState otherState) {
    return new StatePairFinal(thisState, otherState);
  }

  /**
   * @return a TransitionKey object consisting of these two states and this operation
   */
  private static TransitionKey tk(ConnectionState thisState, ConnectionState otherState,
      ConnectionOperation operation) {
    return new TransitionKey(sp(thisState, otherState), operation);
  }

  /**
   * Check to see if a userId is actually a valid one
   *
   * @param session the sparsemap session
   * @param userId the userId to check
   * @return the Authorizable represented by this userId
   */
  protected Authorizable checkValidUserId(Session session, String userId)
      throws ConnectionException {
    Authorizable authorizable;
    if (User.ANON_USER.equals(session.getUserId()) || User.ANON_USER.equals(userId)) {
      throw new ConnectionException(403, "Cant make a connection with anonymous.");
    }
    try {
      AuthorizableManager authorizableManager = session.getAuthorizableManager();
      authorizable = authorizableManager.findAuthorizable(userId);
      if (authorizable != null && authorizable.getId().equals(userId)) {
        return authorizable;
      }
    } catch (StorageClientException e) {
      // general repo failure
      throw new ConnectionException(500, e.getMessage(), e);
    } catch (Exception e) {
      // other failures return false
      LOGGER.info("Failure checking for valid user (" + userId + "): " + e);
      throw new ConnectionException(404, "User " + userId + " does not exist.");
    }
    throw new ConnectionException(404, "User " + userId + " does not exist.");
  }

  // SERVICE INTERFACE METHODS

  /**
   * {@inheritDoc}
   *
   * @see org.sakaiproject.nakamura.api.connections.ConnectionManager#connect(java.util.Map,
   * org.sakaiproject.nakamura.api.lite.Session,
   * String, String, org.sakaiproject.nakamura.api.connections.ConnectionOperation)
   */
  public boolean connect(Map<String, String[]> requestParameters, Session session,
      String thisUserId, String otherUserId, ConnectionOperation operation)
      throws ConnectionException {

    if (thisUserId.equals(otherUserId)) {
      throw new ConnectionException(
          400,
          "A user cannot operate on their own connection, this user and the other user are the same");
    }

    // fail if the supplied users are invalid
    Authorizable thisAu = checkValidUserId(session, thisUserId);
    Authorizable otherAu = checkValidUserId(session, otherUserId);

    Session adminSession = null;
    try {
      adminSession = repository.loginAdministrative();

      // get the contact userstore nodes
      ContactConnection thisNode = connectionStorage.getOrCreateContactConnection(thisAu, otherAu);
      ContactConnection otherNode = connectionStorage.getOrCreateContactConnection(otherAu, thisAu);
      if ( thisNode == null ) {
        throw new ConnectionException(400,"Failed to connect users, no connection for "+thisUserId );
      }
      if ( otherNode == null ) {
        throw new ConnectionException(400,"Failed to connect users, no connection for "+otherUserId );
      }

      // check the current states
      ConnectionState thisState = thisNode.getConnectionState();
      ConnectionState otherState = otherNode.getConnectionState();
      StatePair sp = stateMap.get(tk(thisState, otherState, operation));
      if (sp == null) {
        throw new ConnectionException(400, "Cannot perform operation "
            + operation.toString() + " on " + thisState.toString() + ":"
            + otherState.toString());
      }

      // A legitimate invitation can set properties on the invited
      // user's view of the connection, including relationship types
      // that differ from those viewed by the inviting user.
      if (operation == ConnectionOperation.invite) {
        handleInvitation(requestParameters, thisNode, otherNode);
      }

      // KERN-763 : Connections need to be "stored" in groups.
      StatePairFinal spAccepted = new StatePairFinal(ACCEPTED, ACCEPTED);
      if (sp.equals(spAccepted)) {
        addUserToGroup(thisAu, otherAu, adminSession);
        addUserToGroup(otherAu, thisAu, adminSession);
        // KERN-1696 make it so that everyone can read someone's list of contacts
        // TODO Should the rule be that reading a contact list is limited to people who are in that contact list?
        AccessControlManager accessControlManager = adminSession.getAccessControlManager();
        AclModification[] aclMods = new AclModification[] { new AclModification(AclModification.grantKey(Group.EVERYONE),
            Permissions.CAN_READ.getPermission(), Operation.OP_REPLACE),
            new AclModification(AclModification.grantKey(User.ANON_USER),
                Permissions.CAN_READ.getPermission(), Operation.OP_REPLACE) };
        accessControlManager.setAcl(Security.ZONE_CONTENT, ConnectionUtils.getConnectionPathBase(thisAu.getId()), aclMods);
        accessControlManager.setAcl(Security.ZONE_CONTENT, ConnectionUtils.getConnectionPathBase(otherAu.getId()), aclMods);
      } else {
        // This might be an existing connection that needs to be removed
        removeUserFromGroup(thisAu, otherAu, adminSession);
        removeUserFromGroup(otherAu, thisAu, adminSession);
      }

      sp.transition(thisNode, otherNode);

      connectionStorage.saveContactConnectionPair(thisNode, otherNode);

      if (operation == ConnectionOperation.invite) {
        throw new ConnectionException(200, "Invitation made between "
            + thisNode.toString() + " and " + otherNode.toString());
      }
    } catch (StorageClientException e) {
      throw new ConnectionException(500, e.getMessage(), e);
    } catch (AccessDeniedException e) {
      throw new ConnectionException(403, e.getMessage(), e);
    } finally {
      if (adminSession != null) {
        // destroy the admin session
        try {
          adminSession.logout();
        } catch (ClientPoolException e) {
          LOGGER.error(e.getMessage(),e);
        }
      }
    }
    return true;
  }

  public boolean writeConnectionInfo(ExtendedJSONWriter exWriter, Session session,
      String thisUserId, String otherUserId) throws AccessDeniedException,
      StorageClientException, ConnectionException {
    Authorizable thisUser = session.getAuthorizableManager().findAuthorizable(thisUserId);
    Authorizable otherUser = session.getAuthorizableManager().findAuthorizable(otherUserId);
    //add contact information if appropriate
    ContactConnection connection = connectionStorage.getContactConnection(thisUser, otherUser);
    if (connection != null) {
      // add sakai:state and sakai:types
      try {
        exWriter.key(SAKAI_CONNECTION_STATE);
        exWriter.value(connection.getConnectionState().toString(), false);
        exWriter.key(SAKAI_CONNECTION_TYPES);
        exWriter.value(connection.getConnectionTypes(), false);
      } catch (JSONException e) {
        throw new ConnectionException(500, e);
      }
    }
    return connection != null;
  }

  /**
   * Removes a member from a group
   *
   * @param thisAu
   *          The {@link Authorizable authorizable} who owns the group.
   * @param otherAu
   *          The {@link Authorizable authorizable} who needs to be removed from the
   *          contact group.
   * @param session
   *          A session that can be used to modify a group.
   * @throws StorageClientException 
   * @throws AccessDeniedException 
   */
  protected void removeUserFromGroup(Authorizable thisAu, Authorizable otherAu,
      Session session) throws StorageClientException, AccessDeniedException {
    if ( otherAu != null && thisAu != null) {
      AuthorizableManager authorizableManager = session.getAuthorizableManager();
      Group g = (Group) authorizableManager.findAuthorizable("g-contacts-" + thisAu.getId());
      g.removeMember(otherAu.getId());
      thisAu.removeProperty("contactsCount");
      authorizableManager.updateAuthorizable(g);
      authorizableManager.updateAuthorizable(thisAu);
    }
  }

  /**
   * Adds one user to another user his connection group.
   *
   * @param thisAu
   *          The base user who is adding a contact.
   * @param otherAu
   *          The user that needs to be added to the group.
   * @param session
   *          The session that can be used to locate and manipulate the group
   * @throws StorageClientException 
   * @throws AccessDeniedException 
   */
  protected void addUserToGroup(Authorizable thisAu, Authorizable otherAu, Session session) throws StorageClientException, AccessDeniedException {
    AuthorizableManager authorizableManager = session.getAuthorizableManager();
    Group g = (Group) authorizableManager.findAuthorizable("g-contacts-" + thisAu.getId());
    g.addMember(otherAu.getId());
    thisAu.removeProperty("contactsCount");
    authorizableManager.updateAuthorizable(g);
    authorizableManager.updateAuthorizable(thisAu);
  }

  /**
   * {@inheritDoc}
   *
   * @see org.sakaiproject.nakamura.api.connections.ConnectionManager#getConnectedUsers(org.sakaiproject.nakamura.api.lite.Session,
   *      java.lang.String, org.sakaiproject.nakamura.api.connections.ConnectionState)
   */
  public List<String> getConnectedUsers(Session session, String user, ConnectionState state) throws ConnectionException {
    return connectionStorage.getConnectedUsers(session, user, state);
  }

  protected void handleInvitation(Map<String, String[]> requestProperties, ContactConnection fromNode, ContactConnection toNode)  {
    Set<String> toRelationships = new HashSet<String>();
    Set<String> fromRelationships = new HashSet<String>();
    Map<String, Object> sharedProperties = new HashMap<String, Object>();
    if (requestProperties != null) {
      for (Entry<String, String[]> rp : requestProperties.entrySet()) {
        String key = rp.getKey();
        String[] values = rp.getValue();
        if (ConnectionConstants.PARAM_FROM_RELATIONSHIPS.equals(key)) {
          fromRelationships.addAll(Arrays.asList(values));
        } else if (ConnectionConstants.PARAM_TO_RELATIONSHIPS.equals(key)) {
          toRelationships.addAll(Arrays.asList(values));
        } else if (ConnectionConstants.SAKAI_CONNECTION_TYPES.equals(key)) {
          fromRelationships.addAll(Arrays.asList(values));
          toRelationships.addAll(Arrays.asList(values));
        } else {
          if (values.length == 1) {
            sharedProperties.put(key, values[0]);
          } else if (values.length > 1) {
            sharedProperties.put(key, values);
          }
        }
      }
    }
    fromNode.addProperties(sharedProperties);
    fromNode.setConnectionTypes(fromRelationships);

    toNode.addProperties(sharedProperties);
    toNode.setConnectionTypes(toRelationships);
  }

}
