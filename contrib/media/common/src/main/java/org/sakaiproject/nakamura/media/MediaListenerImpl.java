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
 * specific language governing permissions and limitations under
 * the License.
 */
package org.sakaiproject.nakamura.media;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Map;
import java.util.HashMap;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventHandler;
import org.sakaiproject.nakamura.api.media.MediaService;
import org.sakaiproject.nakamura.api.activemq.ConnectionFactoryService;
import org.sakaiproject.nakamura.api.files.FileUploadHandler;
import org.sakaiproject.nakamura.api.lite.Repository;
import org.sakaiproject.nakamura.api.media.MediaListener;
import org.sakaiproject.nakamura.api.lite.content.ContentManager;
import org.sakaiproject.nakamura.api.lite.content.Content;
import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.files.FilesConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FilenameUtils;


/*
 * use immediate = true to start the worker pool up during server start rather than leaving that
 * for the first caller to incur the cost of doing.
 */
@Component(immediate = true, metatype = true)
@Service({ MediaListener.class, EventHandler.class, FileUploadHandler.class })
@Properties({
  @Property(name = "event.topics", value = "org/sakaiproject/nakamura/lite/content/UPDATED")
})
public class MediaListenerImpl implements MediaListener, EventHandler, FileUploadHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MediaListenerImpl.class);

  private String QUEUE_NAME = "MEDIA";

  @Reference
  private ConnectionFactoryService connectionFactoryService;
  private ConnectionFactory connectionFactory;

  @Reference
  protected Repository sparseRepository;

  @Reference
  protected MediaService mediaService;


  private MediaCoordinator ucbMediaCoordinator;

  private Map<String,String> mimeTypeToExtension;


  private Map<String,String> parseMimeTypes()
  {
    InputStream in = getClass().getResourceAsStream("/mime.types");

    if (in == null) {
      throw new RuntimeException("Couldn't find resource 'mime.types'.");
    }

    Map<String,String> result = new HashMap<String,String>();

    try {
      BufferedReader rdr = new BufferedReader(new InputStreamReader(in));
      try {
        String line;
        while ((line = rdr.readLine()) != null) {
          if (line.startsWith("#")) {
            continue;
          }

          String[] bits = line.split("\\s+");
          result.put(bits[0], bits[1]);
        }
      } finally {
        rdr.close();
      }

      return result;
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  @Activate
  @Modified
  protected void activate(ComponentContext context) {
    LOGGER.info("Activating Media bundle");

    mimeTypeToExtension = parseMimeTypes();

    connectionFactory = connectionFactoryService.getDefaultPooledConnectionFactory();

    ucbMediaCoordinator = new MediaCoordinator(connectionFactory,
                                               QUEUE_NAME,
                                               sparseRepository,
                                               mediaService);
    ucbMediaCoordinator.start();
  }

  @Deactivate
  protected void deactivate(ComponentContext context) {
    LOGGER.info("Deactivating Media bundle");

    if (ucbMediaCoordinator != null) {
      LOGGER.info("Shutting down Media coordinator thread...");
      ucbMediaCoordinator.shutdown();
      LOGGER.info("Done");
    }
  }

  // --------------- MediaListener interface -----------------------------------
  @Override
  public void contentUpdated(String pid) {
    LOGGER.info("Content updated: {}", pid);

    try {
      Connection conn = connectionFactory.createConnection();
      Session jmsSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue mediaQueue = jmsSession.createQueue(QUEUE_NAME);

      MessageProducer producer = jmsSession.createProducer(mediaQueue);

      producer.send(MediaUtils.message(jmsSession, "pid", pid));

      producer.close();
      jmsSession.close();
      conn.close();

    } catch (JMSException e) {
      LOGGER.error("Failed when adding content PID to JMS queue: {}", e);
      e.printStackTrace();
    }
  }

  // --------------- EventHandler interface -----------------------------------
  @Override
  public void handleEvent(Event event) {
    LOGGER.info("Got event: {}", event);

    String path = (String) event.getProperty("path");
    String op = (String) event.getProperty("op");
    String resourceType = (String) event.getProperty("resourceType");

    // We're only interested in top-level content PIDs here. Don't bother
    // us about /activity nodes or other children.
    if (path.indexOf("/") == -1 && "update".equals(op)
        && "sakai/pooled-content".equals(resourceType)) {

      contentUpdated(path);
    }
  }

  // --------------- FileUploadHandler interface ------------------------------
  @Override
  public void handleFile(Map<String, Object> results, String poolId,
      InputStream inputStream, String userId, boolean isNew) throws IOException {

    try {
      org.sakaiproject.nakamura.api.lite.Session adminSession = sparseRepository.loginAdministrative();
      ContentManager cm = adminSession.getContentManager();
      Content obj = cm.get(poolId);

      String mimeType = (String)obj.getProperty(FilesConstants.POOLED_CONTENT_MIMETYPE);
      String extension = mimeTypeToExtension.get(mimeType);

      LOGGER.info("STUFF: {} AND {}", mimeType, extension);

      if (mimeType != null && extension != null &&
          mediaService.acceptsFileType(mimeType, extension)) {
        obj = cm.get(poolId);
        obj.setProperty(FilesConstants.POOLED_CONTENT_MIMETYPE,
                        mediaService.getMimeType());
        obj.setProperty("media:extension", extension);

        cm.update(obj);
      }

      contentUpdated(poolId);
    } catch (ClientPoolException e) {
      LOGGER.info("ClientPoolException when handling file: {}", e);
      e.printStackTrace();
    } catch (StorageClientException e) {
      LOGGER.info("StorageClientException when handling file: {}", e);
      e.printStackTrace();
    } catch (AccessDeniedException e) {
      LOGGER.info("AccessDeniedException when handling file: {}", e);
      e.printStackTrace();
    }

  }
}
