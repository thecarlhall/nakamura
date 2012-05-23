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
package org.sakaiproject.nakamura.api.connections;

import org.osgi.service.event.Event;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class ConnectionEventUtil {

  public static final String TOPIC_BASE = "org/sakaiproject/nakamura/api/connections/";
  
  // TODO: we can do cooler things here, like ACCEPTED, REJECTED, etc... another time.
  public static final String TOPIC_CREATE = TOPIC_BASE + "CREATE";
  public static final String TOPIC_UPDATE = TOPIC_BASE + "UPDATE";
  
  // the unique identifier of an event is loosely considered to be 'path', and QueueManager will not act on one if this property does not exist.
  public static final String FIELD_KEY = "path";
  public static final String FIELD_CONNECTION = TOPIC_BASE + "FIELD_CONNECTION";
  public static final String FIELD_CONNECTION_BEFORE = TOPIC_BASE + "FIELD_CONNECTION_BEFORE";
  
  public static Event createCreateConnectionEvent(ContactConnection connection) {
    if (connection == null)
      throw new IllegalArgumentException("Cannot create CREATE event from null connection.");
    
    Map<String, Object> props = new HashMap<String, Object>();
    props.put(FIELD_KEY, connection.getKey());
    props.put(FIELD_CONNECTION, connection);
    return new Event(TOPIC_CREATE, props);
  }
  
  public static Event createUpdateConnectionEvent(ContactConnection oldConnection,
      ContactConnection newConnection) {
    if (oldConnection == null)
      throw new IllegalArgumentException("Cannot create CREATE event from null oldConnection.");
    if (newConnection == null)
      throw new IllegalArgumentException("Cannot create CREATE event from null newConnection.");
    if (!oldConnection.getKey().equals(newConnection.getKey()))
      throw new IllegalArgumentException(String.format(
          "Cannot create UPDATE event for oldConnection and newConnection with different keys: '%s' and '%s'",
          oldConnection.getKey(), newConnection.getKey()));
    
    Map<String, Object> props = new HashMap<String, Object>();
    props.put(FIELD_KEY, newConnection.getKey());
    props.put(FIELD_CONNECTION, newConnection);
    props.put(FIELD_CONNECTION_BEFORE, oldConnection);
    return new Event(TOPIC_UPDATE, props);
  }
  
}
