package org.sakaiproject.nakamura.media;


import org.sakaiproject.nakamura.api.lite.content.Content;
import org.sakaiproject.nakamura.api.lite.content.ContentManager;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;

import org.sakaiproject.nakamura.api.files.FilesConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;


class VersionManager {

  private ContentManager contentManager;


  public VersionManager(ContentManager cm) {
    this.contentManager = cm;
  }


  // Get the versions metadata for 'pid' from the oldest version to newest.
  public List<Version> getVersionsMetadata(String pid) throws StorageClientException, AccessDeniedException {
    List<Content> versions = new ArrayList<Content>();

    Content latest = contentManager.get(pid);

    if (latest == null) {
      return null;
    }

    versions.add(latest);

    for (String versionId : contentManager.getVersionHistory(pid)) {
      versions.add(contentManager.getVersion(pid, versionId));
    }

    // So this is strange.  Each object in the version history list has the
    // right binary blob for its version, but its metadata corresponds to
    // the previous version.  For example, version 5 will have the bytes of
    // the fifth uploaded file, but the title/description/etc. of the 4th
    // version.
    List<Version> result = new ArrayList<Version>();
    for (int i = 1; i < versions.size(); i++) {
      Content current = versions.get(i);
      Content last = versions.get(i - 1);

      String[] tags = (String[])last.getProperty("sakai:tags");

      if (tags == null) {
        tags = new String[0];
      }

      result.add(new Version(pid,
                             (String)current.getProperty("_id"),
                             (String)last.getProperty("sakai:pooled-content-file-name"),
                             (String)last.getProperty("sakai:description"),
                             (String)current.getProperty("media:extension"),
                             (String)current.getProperty(FilesConstants.POOLED_CONTENT_MIMETYPE),
                             (String)current.getProperty("media:tempStoreLocation"),
                             tags,
                             contentManager));
    }

    Collections.reverse(result);

    return result;
  }
}

