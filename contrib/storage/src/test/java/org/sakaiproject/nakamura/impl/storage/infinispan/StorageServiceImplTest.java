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

import org.junit.Test;
import org.sakaiproject.nakamura.api.storage.CloseableIterator;
import org.sakaiproject.nakamura.api.storage.Entity;

/**
 *
 */
public class StorageServiceImplTest {

  @Test
  public void testFindAll() {
    StorageServiceImpl service = new StorageServiceImpl();
    service.getDao(GenericEntity.class).update(new GenericEntity("key", "prop1"));
    CloseableIterator<Entity> allEntities = service.findAll();
    Entity found = allEntities.next();
    
    Assert.assertFalse(allEntities.hasNext());
    Assert.assertNotNull(found);
    Assert.assertTrue(found instanceof GenericEntity);
    Assert.assertEquals("key", ((GenericEntity)found).getKey());
    Assert.assertEquals("prop1", ((GenericEntity)found).getProp1());
  }
}
