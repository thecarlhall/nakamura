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
package org.sakaiproject.nakamura.connections.search;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.commons.lang.StringUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.osgi.service.event.Event;
import org.sakaiproject.nakamura.api.connections.ConnectionEventUtil;
import org.sakaiproject.nakamura.api.connections.ConnectionStorage;
import org.sakaiproject.nakamura.api.connections.ContactConnection;
import org.sakaiproject.nakamura.api.lite.Session;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.Permissions;
import org.sakaiproject.nakamura.api.lite.accesscontrol.Security;
import org.sakaiproject.nakamura.api.lite.authorizable.Authorizable;
import org.sakaiproject.nakamura.api.lite.authorizable.AuthorizableManager;
import org.sakaiproject.nakamura.api.solr.IndexingHandler;
import org.sakaiproject.nakamura.api.solr.QoSIndexHandler;
import org.sakaiproject.nakamura.api.solr.RepositorySession;
import org.sakaiproject.nakamura.api.solr.SolrServerService;
import org.sakaiproject.nakamura.api.solr.TopicIndexer;
import org.sakaiproject.nakamura.api.storage.StorageEventUtil;
import org.sakaiproject.nakamura.util.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <p>Indexing handler for contact connections between two users.</p>
 * <p>The fields that are indexed are:<br/>
 * <ul>
 * <li>connection status: state</li>
 * <li>contact's username: name</li>
 * <li>contact's first name: firstName</li>
 * <li>contact's last name: lastName</li>
 * <li>contact's email: email</li>
 * </ul>
 */
@Component(immediate = true)
public class ConnectionIndexingHandler implements IndexingHandler, QoSIndexHandler {

  private static final Logger logger = LoggerFactory.getLogger(ConnectionIndexingHandler.class);
  
  private static final Set<String> FLATTENED_PROPS = ImmutableSet.of("name", "firstName",
      "lastName", "email");

  @Reference
  protected ConnectionStorage connectionStorage;

  @Reference
  protected TopicIndexer topicIndexer;
  
  @Reference
  protected SolrServerService solrServerService;
  
  @Activate
  public void activate(Map<String, Object> properties) throws Exception {
    topicIndexer.addHandler(StorageEventUtil.TOPIC_REFRESH_DEFAULT, this);
    topicIndexer.addHandler(ConnectionEventUtil.TOPIC_UPDATE, this);
    topicIndexer.addHandler(ConnectionEventUtil.TOPIC_CREATE, this);
  }

  @Deactivate
  public void deactivate(Map<String, Object> properties) {
    topicIndexer.removeHandler(StorageEventUtil.TOPIC_REFRESH_DEFAULT, this);
    topicIndexer.removeHandler(ConnectionEventUtil.TOPIC_UPDATE, this);
    topicIndexer.removeHandler(ConnectionEventUtil.TOPIC_CREATE, this);
  }

  /**
   * {@inheritDoc}
   *
   * @see org.sakaiproject.nakamura.api.solr.QoSIndexHandler#getTtl(org.osgi.service.event.Event)
   */
  public int getTtl(Event event) {
    // have to be > 0 based on the logic in ContentEventListener.
    // see org.sakaiproject.nakamura.solr.Utils.defaultMax(int)
    return 50;
  }

  /**
   * {@inheritDoc}
   *
   * @see org.sakaiproject.nakamura.api.solr.IndexingHandler#getDocuments(org.sakaiproject.nakamura.api.solr.RepositorySession, org.osgi.service.event.Event)
   */
  public Collection<SolrInputDocument> getDocuments(RepositorySession repoSession, Event event) {
    String path = (String) event.getProperty(IndexingHandler.FIELD_PATH);

    logger.info("Indexing connections at path {}", path);
    List<SolrInputDocument> documents = Lists.newArrayList();
    if (!StringUtils.isBlank(path)) {
      try {
        Session session = repoSession.adaptTo(Session.class);
        String sourceName = PathUtils.getAuthorizableId(StorageClientUtils.getParentObjectPath(path));
        String contactName = StorageClientUtils.getObjectName(path);
        AuthorizableManager am = session.getAuthorizableManager();
        Authorizable sourceAuth = am.findAuthorizable(sourceName);
        Authorizable contactAuth = am.findAuthorizable(contactName);

        ContactConnection connection = connectionStorage.getContactConnection(sourceAuth, contactAuth);
        
        if (connection != null) {
          SolrInputDocument doc = new SolrInputDocument(); 
          doc.addField(FIELD_ID, connection.getKey());
          doc.addField(FIELD_PATH, connection.getKey());
          doc.addField("contactstorepath", StorageClientUtils.getParentObjectPath(path));
          doc.addField("state", connection.getConnectionState().toString());
          doc.addField("resourceType", "sakai/contact");
          
          // needed for permission lookup at search-time
          addReaders(doc, session, path);
          
          // flatten out the contact so we can search it
          Map<String, Object> contactProps = contactAuth.getSafeProperties();
          for (String prop : FLATTENED_PROPS) {
            Object value = contactProps.get(prop);
            if ( value != null ) {
              doc.addField(prop, value);
            }
          }

          documents.add(doc);
        } else {
          logger.debug("Did not index {}: Contact Auth == {}",
              new Object[] { path, contactAuth });
        }
      } catch (StorageClientException e) {
        logger.error(e.getMessage(), e);
      } catch (AccessDeniedException e) {
        logger.error(e.getMessage(), e);
      }
    }
    logger.debug("Got documents {} ", documents);
    return documents;
  }
  
  /**
   * {@inheritDoc}
   *
   * @see org.sakaiproject.nakamura.api.solr.IndexingHandler#getDeleteQueries(org.sakaiproject.nakamura.api.solr.RepositorySession,
   *      org.osgi.service.event.Event)
   */
  public Collection<String> getDeleteQueries(RepositorySession repositorySession,
      Event event) {
    List<String> retval = Collections.emptyList();
    logger.debug("GetDelete for {} ", event);
    String path = (String) event.getProperty(IndexingHandler.FIELD_PATH);
    if (ContactConnection.class.equals(event.getProperty(StorageEventUtil.FIELD_ENTITY_CLASS))) {
      retval = ImmutableList.of("id:" + ClientUtils.escapeQueryChars(path));
    }
    return retval;
  }
  
  private void addReaders(SolrInputDocument doc, Session session, String path) throws StorageClientException {
    String readers[] = session.getAccessControlManager().findPrincipals(Security.ZONE_CONTENT,
        path, Permissions.CAN_READ.getPermission(), true);
    for (String reader : readers) {
      doc.addField(FIELD_READERS, reader);
    }
  }
}
