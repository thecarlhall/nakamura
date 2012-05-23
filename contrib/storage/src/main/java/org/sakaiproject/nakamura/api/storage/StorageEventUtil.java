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

import org.osgi.service.event.Event;
import org.osgi.service.event.EventAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Constants for handling storage events. Note that events like CREATE / UPDATE / DELETE
 * might not be handled at this level. Rather, higher-level services would likely fire
 * more coherent application-level events to be acted upon.
 * 
 * It is possible that not even "REFRESH" will be handled here in the future, as it
 * may be handled in a single event that is subscribed to by individual high-level
 * services that update their respective index handlers. Until then, this handles the
 * existing refresh paradigm.
 * 
 */
public class StorageEventUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageEventUtil.class);
  
  public static final String TOPIC_BASE = "org/sakaiproject/nakamura/api/storage/";
  public static final String TOPIC_REFRESH = "REFRESH";
  public static final String TOPIC_REFRESH_DEFAULT = TOPIC_BASE + TOPIC_REFRESH;
  
  // the unique identifier of an event is loosely considered to be 'path', and QueueManager will not act on one if this property does not exist.
  public static final String FIELD_KEY = "path";
  public static final String FIELD_ENTITY_CLASS = "org/sakaiproject/nakamura/api/storage/StorageEventManager/FIELD_ENTITY_CLASS";
  public static final String FIELD_ENTITY_BEFORE = "org/sakaiproject/nakamura/api/storage/StorageEventManager/FIELD_ENTITY_BEFORE";
  public static final String FIELD_ACTOR_USER_ID = "org/sakaiproject/nakamura/api/storage/StorageEventManager/FIELD_ACTOR_USER_ID";
  
  public static final Event createStorageEvent(String topic, String actorUserId, Entity newEntity, Entity oldEntity) {
    if (topic == null)
      throw new IllegalArgumentException("Cannot create event from null topic.");
    if (actorUserId == null)
      throw new IllegalArgumentException("Cannot create event from null actorUserId.");
    if (newEntity == null)
      throw new IllegalArgumentException("Cannot create event from null newEntity.");
    if (oldEntity != null && !newEntity.getClass().equals(oldEntity.getClass()))
      throw new IllegalArgumentException("If oldEntity and newEntity are specified, they must be the same class.");
    
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put(FIELD_KEY, newEntity.getKey());
    properties.put(FIELD_ENTITY_CLASS, newEntity.getClass());
    properties.put(FIELD_ENTITY_BEFORE, oldEntity);
    properties.put(FIELD_ACTOR_USER_ID, actorUserId);
    
    return new Event(topic, properties);
  }
  
  public static final void refreshAllEntities(StorageService storageService,
      EventAdmin eventAdmin, String actorUserId, boolean async) {
    
    if (async) {
      LOGGER.info("Beginning refresh of all entities asynchronously.");
    } else {
      LOGGER.info("Beginning refresh of all entities synchronously.");
    }
    
    long count = 0;
    
    CloseableIterator<Entity> entities = storageService.findAll();
    while (entities.hasNext()) {
      Entity entity = entities.next();
      Event event = StorageEventUtil.createStorageEvent(StorageEventUtil.TOPIC_REFRESH_DEFAULT,
          "admin", entity, null);
      
      // fire the event
      if (async) {
        eventAdmin.postEvent(event);
      } else {
        eventAdmin.sendEvent(event);
      }
      
      count++;
    }
    
    if (async) {
      LOGGER.info("Finished firing {} refresh events asynchronously", count);
    } else {
      LOGGER.info("Finished firing {} refresh events synchronously", count);
    }
  }
}
