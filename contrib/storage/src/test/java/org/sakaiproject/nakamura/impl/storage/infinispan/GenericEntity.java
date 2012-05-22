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
package org.sakaiproject.nakamura.impl.storage.infinispan;

import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.ProvidedId;
import org.sakaiproject.nakamura.api.storage.Entity;

/**
 *
 */
@Indexed @ProvidedId
public class GenericEntity implements Entity {

  private String key;
  private String prop1;
  
  public GenericEntity(String key, String prop1) {
    this.key = key;
    this.prop1 = prop1;
  }
  
  /**
   * @return the key
   */
  @Override
  public String getKey() {
    return key;
  }
  
  /**
   * @param key the key to set
   */
  public void setKey(String key) {
    this.key = key;
  }
  
  /**
   * @return the prop1
   */
  public String getProp1() {
    return prop1;
  }
  
  /**
   * @param prop1 the prop1 to set
   */
  public void setProp1(String prop1) {
    this.prop1 = prop1;
  }

  
}
