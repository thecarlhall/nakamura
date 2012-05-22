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

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.sakaiproject.nakamura.api.storage.EntityDao;
import org.sakaiproject.nakamura.api.storage.StorageService;

/**
 *
 */
public class EntityDaoTest {

  private StorageService service;
  private EntityDao<GenericEntity> dao;
  
  @Before
  public void setup() {
    service = new StorageServiceImpl();
    dao = service.getDao(GenericEntity.class);
  }
  
  @Test
  public void testWriteAndRead() {
    GenericEntity entityTransient = new GenericEntity("key", "value1");
    dao.update(entityTransient);
    
    // verify that the entity persisted and properties are accurate.
    GenericEntity entityPersistent = dao.get("key");
    Assert.assertNotNull(entityPersistent);
    Assert.assertEquals("key", entityPersistent.getKey());
    Assert.assertEquals("value1", entityPersistent.getProp1());
  }
}
