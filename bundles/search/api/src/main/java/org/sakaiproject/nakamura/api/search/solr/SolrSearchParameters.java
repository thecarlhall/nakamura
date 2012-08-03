package org.sakaiproject.nakamura.api.search.solr;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.client.solrj.SolrQuery;

import java.util.HashMap;
import java.util.Map;

public class SolrSearchParameters {

  private long page;
  private long recordsPerPage;
  private SolrQuery.ORDER order = SolrQuery.ORDER.asc;
  private String path;
  private String sortOn;

  public SolrSearchParameters() {
    page = 0;
    recordsPerPage = Integer.MAX_VALUE;
  }

  public SolrSearchParameters(long page, long recordsPerPage, SolrQuery.ORDER order, String sortOn) {
    this.page = page;
    this.recordsPerPage = recordsPerPage;
    this.order = order;
    this.sortOn = sortOn;

  }

  public long getRecordsPerPage() {
    return recordsPerPage;
  }

  public void setRecordsPerPage(long recordsPerPage) {
    this.recordsPerPage = recordsPerPage;
  }

  public long getPage() {
    return page;
  }

  public void setPage(long page) {
    this.page = page;
  }

  public SolrQuery.ORDER getOrder() {
    return order;
  }

  public void setOrder(SolrQuery.ORDER order) {
    this.order = order;
  }

  public String getSortOn() {
    return sortOn;
  }

  public void setSortOn(String sortOn) {
    this.sortOn = sortOn;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

}
