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
package org.sakaiproject.nakamura.api.storage;

import org.apache.lucene.search.Query;

import java.util.List;

/**
 *
 */
public interface EntityDao<T extends Entity> {

  /**
   * Get the entity with the given key. If the entity does not exist, this will return {@code null}.
   * 
   * @param key
   * @return
   */
  T get(String key);
  
  /**
   * Update the given entity.
   * 
   * @param entity
   * @return
   */
  T update(T entity);

  /**
   * Find all entities that match the given lucene query.
   * 
   * @param query
   * @return
   */
  List<T> findAll(Query query);
  
}
