package org.sakaiproject.nakamura.api.search;

import java.util.List;

public interface DeletedPathsService {

  /**
   * Get a list of the paths that were deleted since the last Solr commit across all nodes
   * in the cluster. Escapes the paths to make sure they are safe for consumption in a
   * query.
   *
   * @return {@link List} of unescaped path strings.
   */
  List<String> getDeletedPaths();

  /**
   * Get a list of the paths that were deleted since the last Solr commit across all nodes
   * in the cluster. Escapes the paths to make sure they are safe for consumption in a
   * query.
   *
   * @param queryLanguage The query language the paths should be escaped for.
   * @return {@link List} of escaped path strings. 
   */
  List<String> getEscapedDeletedPaths(String queryLanguage);
}