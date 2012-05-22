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

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.QueryIterator;
import org.infinispan.query.Search;
import org.sakaiproject.nakamura.api.storage.CloseableIterator;
import org.sakaiproject.nakamura.api.storage.Entity;
import org.sakaiproject.nakamura.api.storage.EntityDao;
import org.sakaiproject.nakamura.api.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
@Service
@Component
public class StorageServiceImpl implements StorageService {

  private static Logger LOGGER = LoggerFactory.getLogger(StorageServiceImpl.class);
  
  private Cache<String, Entity> entityCache;
  
  public StorageServiceImpl() {
    activate(null);
  }
  
  @Activate
  public void activate(Map<String, Object> properties) {
    GlobalConfigurationBuilder globalCfg = new GlobalConfigurationBuilder();
    globalCfg.classLoader(getClass().getClassLoader());
    
    DefaultCacheManager container = new DefaultCacheManager(globalCfg.build(), true);
    
    ConfigurationBuilder cfg = new ConfigurationBuilder();
    cfg.classLoader(getClass().getClassLoader());
    cfg.indexing().enable().indexLocalOnly(true);
    container.defineConfiguration("EntityCache", cfg.build());
    
    entityCache = container.getCache("EntityCache");
  }
  
  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.storage.StorageService#getDao(java.lang.Class)
   */
  @SuppressWarnings("unchecked")
  @Override
  public <T extends Entity> EntityDao<T> getDao(Class<T> clazz) {
    return new GenericEntityDao<T>((Cache<String, T>) entityCache, clazz);
  }

  @Override
  public CloseableIterator<Entity> findAll() {
    CacheQuery query = Search.getSearchManager(entityCache).getQuery(new MatchAllDocsQuery());
    final QueryIterator i = query.lazyIterator();
    return new CloseableIterator<Entity>() {

      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public Entity next() {
        Entity entity = (Entity) i.next();
        
        if (entity != null)
          LOGGER.info("Found entity: {}", entity.getKey());
        
        return entity;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("Cannot remove document from query iterator.");
      }

      @Override
      public void close() throws IOException {
        i.close();
      }
      
    };
  }

}
