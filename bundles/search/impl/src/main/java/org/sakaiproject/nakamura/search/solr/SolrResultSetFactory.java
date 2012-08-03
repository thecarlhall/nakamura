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
package org.sakaiproject.nakamura.search.solr;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.commons.osgi.PropertiesUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.GroupParams;
import org.perf4j.aop.Profiled;
import org.sakaiproject.nakamura.api.lite.Session;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.lite.authorizable.Authorizable;
import org.sakaiproject.nakamura.api.lite.authorizable.AuthorizableManager;
import org.sakaiproject.nakamura.api.lite.authorizable.Group;
import org.sakaiproject.nakamura.api.lite.authorizable.User;
import org.sakaiproject.nakamura.api.search.DeletedPathsService;
import org.sakaiproject.nakamura.api.search.solr.Query;
import org.sakaiproject.nakamura.api.search.solr.ResultSetFactory;
import org.sakaiproject.nakamura.api.search.solr.SolrSearchException;
import org.sakaiproject.nakamura.api.search.solr.SolrSearchParameters;
import org.sakaiproject.nakamura.api.search.solr.SolrSearchResultSet;
import org.sakaiproject.nakamura.api.search.solr.SolrSearchUtil;
import org.sakaiproject.nakamura.api.solr.SolrServerService;
import org.sakaiproject.nakamura.util.telemetry.TelemetryCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.sakaiproject.nakamura.api.search.solr.SolrSearchConstants.DEFAULT_PAGED_ITEMS;
import static org.sakaiproject.nakamura.api.search.solr.SolrSearchConstants.PARAMS_ITEMS_PER_PAGE;
import static org.sakaiproject.nakamura.api.search.solr.SolrSearchConstants.PARAMS_PAGE;

/**
 *
 */
@Component(metatype = true)
@Service
@Property(name = "type", value = Query.SOLR)

public class SolrResultSetFactory implements ResultSetFactory {
  @Property(longValue = 100L)
  private static final String VERY_SLOW_QUERY_TIME = "verySlowQueryTime";
  @Property(longValue = 10L)
  private static final String SLOW_QUERY_TIME = "slowQueryTime";
  @Property(intValue = 100)
  private static final String DEFAULT_MAX_RESULTS = "defaultMaxResults";
  @Property(value = "POST")
  private static final String HTTP_METHOD = "httpMethod";

  /** only used to mark the logger */
  private final class SlowQueryLogger { }

  private static final Logger LOGGER = LoggerFactory.getLogger(SolrResultSetFactory.class);
  private static final Logger SLOW_QUERY_LOGGER = LoggerFactory.getLogger(SlowQueryLogger.class);

  @Reference
  private SolrServerService solrSearchService;

  @Reference
  private DeletedPathsService deletedPathsService;

  private int defaultMaxResults = 100; // set to 100 to allow testing
  private long slowQueryThreshold;
  private long verySlowQueryThreshold;
  private METHOD queryMethod;

  @Activate
  protected void activate(Map<?, ?> props) {
    defaultMaxResults = PropertiesUtil.toInteger(props.get(DEFAULT_MAX_RESULTS),
        defaultMaxResults);
    slowQueryThreshold = PropertiesUtil.toLong(props.get(SLOW_QUERY_TIME), 10L);
    verySlowQueryThreshold = PropertiesUtil.toLong(props.get(VERY_SLOW_QUERY_TIME), 100L);
    queryMethod = METHOD.valueOf(PropertiesUtil.toString(props.get(HTTP_METHOD), "POST"));
  }

  /**
   * @deprecated
   * @param request
   * @param query
   * @param asAnon
   * @return
   * @throws SolrSearchException
   */
  public SolrSearchResultSet processQuery(SlingHttpServletRequest request, Query query, boolean asAnon)
     throws SolrSearchException {
    try {
      final Session session = StorageClientUtils.adaptToSession(request.getResourceResolver().adaptTo(javax.jcr.Session.class));
      final AuthorizableManager authzMgr = session.getAuthorizableManager();
      final Authorizable authorizable = authzMgr.findAuthorizable(asAnon ? User.ANON_USER : request.getRemoteUser());
      final SolrSearchParameters params = SolrSearchUtil.getParametersFromRequest(request);

      return processQuery(session, authorizable, query, params);
    } catch (AccessDeniedException e) {
      LOGGER.error("Access denied for {}", request.getRemoteUser());
      throw new SolrSearchException(403, "access denied");
    } catch (StorageClientException e) {
      LOGGER.error("Error processing query", e);
      throw new SolrSearchException(500, "internal error");
    }
  }

  /**
   * Process a query string to search using Solr.
   *
   * @param session
   * @param authorizable
   * @param query
   * @param params
   * @return
   * @throws SolrSearchException
   */
  @SuppressWarnings("rawtypes")
  public SolrSearchResultSet processQuery(Session session, Authorizable authorizable, Query query,
     SolrSearchParameters params) throws SolrSearchException {
    final boolean asAnon = (authorizable == null || User.ANON_USER.equals(authorizable.getId()));
    final String userId = (authorizable != null) ? authorizable.getId() : User.ANON_USER;

    try {
      // Add reader restrictions to solr fq (filter query) parameter,
      // to prevent "reader restrictions" from affecting the solr score
      // of a document.
      Map<String, Object> originalQueryOptions = query.getOptions();
      Map<String, Object> queryOptions = Maps.newHashMap();
      Object filterQuery = null;

      if (originalQueryOptions != null) {
        // copy from originalQueryOptions in case its backed by a ImmutableMap,
        // which prevents saving of filter query changes.
        queryOptions.putAll(originalQueryOptions);
        if (queryOptions.get(CommonParams.FQ) != null) {
          filterQuery = queryOptions.get(CommonParams.FQ);
        }
      }

      Set<String> filterQueries = Sets.newHashSet();
      // add any existing filter queries to the set
      if (filterQuery != null) {
        if (filterQuery instanceof Object[]) {
          CollectionUtils.addAll(filterQueries, (Object[]) filterQuery);
        } else if (filterQuery instanceof Iterable) {
          CollectionUtils.addAll(filterQueries, ((Iterable) filterQuery).iterator());
        } else {
          filterQueries.add(String.valueOf(filterQuery));
        }
      }

      applyReadersRestrictions(authorizable, session, asAnon, queryOptions);

      // filter out 'excluded' items. these are indexed because we do need to search for
      // some things on the server that the UI doesn't want (e.g. collection groups)
      filterQueries.add("-exclude:true");

      // filter out deleted items
      List<String> deletedPaths = deletedPathsService.getEscapedDeletedPaths(Query.SOLR);
      if (!deletedPaths.isEmpty()) {
        // these are escaped as they are collected
        filterQueries.add("-path:(" + StringUtils.join(deletedPaths, " OR ") + ")");
      }
      // save filterQuery changes
      queryOptions.put(CommonParams.FQ, filterQueries);

      // Ensure proper totals from grouped / collapsed queries.
      if ("true".equals(queryOptions.get(GroupParams.GROUP)) &&
          (queryOptions.get(GroupParams.GROUP_TOTAL_COUNT) == null)) {
        queryOptions.put(GroupParams.GROUP_TOTAL_COUNT, "true");
      }

      SolrQuery solrQuery = buildQuery(query.getQueryString(), queryOptions, params);

      SolrServer solrServer = solrSearchService.getServer();
      if ( LOGGER.isDebugEnabled()) {
        try {
          LOGGER.debug("Performing Query {} ", URLDecoder.decode(solrQuery.toString(),"UTF-8"));
        } catch (UnsupportedEncodingException e) {
        }
      }
      long tquery = System.currentTimeMillis();
      QueryResponse response = doSolrQuery(solrServer, solrQuery);
      tquery = System.currentTimeMillis() - tquery;
      TelemetryCounter.incrementValue("search","SEARCH_PERFORMED",params.getPath());
      try {
        if ( tquery > verySlowQueryThreshold ) {
          logVerySlow(params.getPath(), solrQuery, tquery);
        } else if ( tquery > slowQueryThreshold ) {
          logSlow(params.getPath(), solrQuery, tquery);
        }
      } catch (UnsupportedEncodingException e) {
      }
      SolrSearchResultSetImpl rs = new SolrSearchResultSetImpl(response);
      if ( LOGGER.isDebugEnabled()) {
        LOGGER.debug("Got {} hits in {} ms", rs.getSize(), response.getElapsedTime());
      }
      return rs;
    } catch (AccessDeniedException e) {
      throw new SolrSearchException(401, e.getMessage());
    } catch (StorageClientException e) {
      throw new SolrSearchException(500, e.getMessage());
    } catch (SolrServerException e) {
        throw new SolrSearchException(500, e.getMessage());
    }
  }

  protected void applyReadersRestrictions(Authorizable authorizable, Session session, boolean asAnon,
     Map<String, Object> queryOptions) throws StorageClientException, AccessDeniedException {
    final String userId = authorizable.getId();
    // apply readers restrictions.
    if (asAnon) {
      queryOptions.put("readers", User.ANON_USER);
    } else {
      if (!User.ADMIN_USER.equals(userId)) {
        AuthorizableManager am = session.getAuthorizableManager();
        Set<String> readers = Sets.newHashSet();
        for (Iterator<Group> gi = authorizable.memberOf(am); gi.hasNext();) {
          readers.add(gi.next().getId());
        }
        readers.add(userId);
        queryOptions.put("readers", StringUtils.join(readers,","));
      }
    }
  }

  @Profiled(tag="search:ResultSet:performed:{$0.resource.path}", el=true)
  private QueryResponse doSolrQuery(SolrServer solrServer, SolrQuery solrQuery) throws SolrServerException {
    return solrServer.query(solrQuery, queryMethod);
  }
  
  @Profiled(tag="search:ResultSet:slow:{$0.resource.path}", el=true)
  private void logSlow(String path, SolrQuery solrQuery, long time)
      throws UnsupportedEncodingException {
    SLOW_QUERY_LOGGER.error("Slow solr query {} ms {} ", time,
        URLDecoder.decode(solrQuery.toString(),"UTF-8"));
    TelemetryCounter.incrementValue("search","SLOW",path);
  }
  
  @Profiled(tag="search:ResultSet:veryslow:{$0.resource.path}", el=true)
  private void logVerySlow(String path, SolrQuery solrQuery, long time)
      throws UnsupportedEncodingException {
    SLOW_QUERY_LOGGER.error("Very slow solr query {} ms {} ", time,
        URLDecoder.decode(solrQuery.toString(),"UTF-8"));
    TelemetryCounter.incrementValue("search","VERYSLOW",path);
  }
  
  /**
   * @param queryString
   * @param options
   * @param params
   * @return
   */
  @SuppressWarnings("unchecked")
  private SolrQuery buildQuery(String queryString, Map<String, Object> options, SolrSearchParameters params) {
    // build the query
    final SolrQuery solrQuery = new SolrQuery(queryString);
    final long page = params.getPage();
    final long recordsPerPage = params.getRecordsPerPage();
    final long offset = page * recordsPerPage;

    solrQuery.setStart((int) offset);
    solrQuery.setRows(Math.min(defaultMaxResults, (int) recordsPerPage));

    // add in some options
    if (options != null) {
      for (Entry<String, Object> option : options.entrySet()) {
        String key = option.getKey();
        Object val = option.getValue();
        if (CommonParams.SORT.equals(key)) {
          parseSort(solrQuery, String.valueOf(val));
        } else if (val instanceof Object[]) {
          for (Object v : (Object[]) val) {
            solrQuery.add(key, String.valueOf(v));
          }
        } else if (val instanceof Iterable) {
          for (Object v : (Iterable<Object>) val) {
            solrQuery.add(key, String.valueOf(v));
          }
        } else {
          solrQuery.add(key, String.valueOf(val));
        }
      }
    }
    return solrQuery;
  }

  /**
   * @param solrQuery
   * @param val
   */
  private void parseSort(SolrQuery solrQuery, String val) {
    /* disable KERN-1855 for now; needs more discussion. */
    // final String[] sortFields = solrQuery.getSortFields();
    // we were using setSortField, now using addSortField; verify state
    // if (sortFields != null && sortFields.length > 0) {
    // throw new IllegalStateException("Expected zero sort fields, found: " + sortFields);
    // }
    // final String[] criteria = val.split(",");
    // for (final String criterion : criteria) {
    // final String[] sort = StringUtils.split(criterion);
    final String[] sort = StringUtils.split(val);

    // use the *_sort fields to have predictable sorting.
    // many of the fields in the index have a lot of processing which
    // causes sorting to yield unpredictable results.
    String sortOn = ("score".equals(sort[0])) ? sort[0] : sort[0] + "_sort";
    switch (sort.length) {
    case 1:
      // solrQuery.addSortField(sort[0], ORDER.asc);
      solrQuery.setSortField(sortOn, ORDER.asc);
      break;
    case 2:
      String sortOrder = sort[1].toLowerCase();
      ORDER o = ORDER.asc;
      try {
        o = ORDER.valueOf(sortOrder);
      } catch (IllegalArgumentException a) {
        if (sortOrder.startsWith("d")) {
          o = ORDER.desc;
        } else {
          o = ORDER.asc;
        }
      }
      // solrQuery.addSortField(sort[0], o);
      solrQuery.setSortField(sortOn, o);
      break;
    default:
      LOGGER.warn("Expected the sort option to be 1 or 2 terms. Found: {}", val);
    }
    // }
  }
}
