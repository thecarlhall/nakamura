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

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.sakaiproject.nakamura.api.storage.Entity;

import java.util.Map;
import java.util.Set;

@Indexed @ProvidedId
@Analyzer(impl=KeywordAnalyzer.class)
public class ContactConnection implements Entity {
  private String key = null;
  private ConnectionState connectionState = ConnectionState.NONE;
  private Set<String> connectionTypes = Sets.newHashSet();
  private Map<String,Object> properties = Maps.newHashMap();
  private String fromUserId = "";
  private String toUserId = "";
  private String firstName = "";
  private String lastName = "";

  public ContactConnection(String key, ConnectionState connectionState, Set<String> connectionTypes,
                           String fromUserId,
                           String toUserId,
                           String firstName,
                           String lastName,
                           Map<String, Object> additionalProperties) {
    this.key = key;
    if (connectionState != null) this.connectionState = connectionState;
    if (connectionTypes != null) this.connectionTypes = connectionTypes;
    if (fromUserId != null) this.fromUserId = fromUserId;
    if (toUserId != null) this.toUserId = toUserId;
    if (firstName != null) this.firstName = firstName;
    if (lastName != null) this.lastName = lastName;
    if (additionalProperties != null) this.properties = additionalProperties;
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.storage.Entity#getKey()
   */
  @Override
  public String getKey() {
    return this.key;
  }
  
  @Field
  public ConnectionState getConnectionState() {
    return connectionState;
  }

  public void setConnectionTypes(Set<String> connectionTypes) {
    this.connectionTypes = connectionTypes;
  }

  public void addProperties(Map<String, Object> additionalProperties) {
    this.properties.putAll(additionalProperties);
  }

  public void setConnectionState(ConnectionState connectionState) {
    this.connectionState = connectionState;
  }

  public Set<String> getConnectionTypes() {
    return connectionTypes;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  @Field
  public String getFromUserId() {
    return fromUserId;
  }

  public String getToUserId() {
    return toUserId;
  }

  public String getFirstName() {
    return firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public Object getProperty(String propertyName) {
    return properties.get(propertyName);
  }

}
