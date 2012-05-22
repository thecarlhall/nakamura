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

import org.apache.lucene.search.Query;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.sakaiproject.nakamura.api.storage.Entity;
import org.sakaiproject.nakamura.api.storage.EntityDao;

import java.util.LinkedList;
import java.util.List;

/**
 *
 */
public class GenericEntityDao<T extends Entity> implements EntityDao<T> {

  private Class<T> type;
  private Cache<String, T> cache;
  
  public GenericEntityDao(Cache<String, T> cache, Class<T> type) {
    this.cache = cache;
    this.type = type;
  }
  
  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.storage.EntityDao#get(java.lang.String)
   */
  @Override
  public T get(String key) {
    return cache.get(key);
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.storage.EntityDao#update(org.sakaiproject.nakamura.api.storage.Entity)
   */
  @Override
  public T update(T entity) {
    cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).put(entity.getKey(), entity);
    return entity;
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.storage.EntityDao#findAll(org.apache.lucene.search.Query)
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<T> findAll(Query luceneQuery) {
    List<T> results = new LinkedList<T>();
    CacheQuery query = Search.getSearchManager(cache).getQuery(luceneQuery, type);
    for (Object result : query.list()) {
      results.add((T) result);
    }
    return results;
  }

}
