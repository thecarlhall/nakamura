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
package org.sakaiproject.nakamura.api.connections;

import org.sakaiproject.nakamura.api.lite.Session;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.util.ExtendedJSONWriter;

import java.util.List;
import java.util.Map;

/**
 * The connection manager manages state changes on connections with friends.
 */
public interface ConnectionManager {

  /**
   * Handle a connection operation from the current user to another user.
   * After an invitation is sent, the current user loses access rights
   * to the target user's view of the connection.
   * @param requestParameters properties (if any) to add to both sides of the connection
   * @param session a Sling session for the current user
   * @param thisUser the id of the user sending the invitation.
   * @param otherUser the id of the user we are connecting to
   * @param operation the operation to perform when connecting (accept, reject, etc.)
   *
   * @return true if normal Sling processing should continue; false if this method took
   *         care of the operation (as will usually be the case with a successful
   *         invitation)
   * @throws ConnectionException 
   */
  boolean connect(Map<String, String[]> requestParameters, Session session,
      String thisUser, String otherUser,
      ConnectionOperation operation)
      throws ConnectionException;


  List<String> getConnectedUsers(
      Session session, String actor,
      ConnectionState accepted) throws ConnectionException;

  /**
   * Writes out connection information between two users. If needed to be wrapped in an
   * object, it is assumed that wrapping is done outside of this method.
   *
   * @param writer
   * @param session
   * @param thisUser The user that starts the connection
   * @param otherUser The user that ends the connection
   * @return true if a connection was found, false otherwise
   */
  boolean writeConnectionInfo(ExtendedJSONWriter writer, Session session,
      String thisUser, String otherUser) throws AccessDeniedException,
      StorageClientException, ConnectionException;
}
