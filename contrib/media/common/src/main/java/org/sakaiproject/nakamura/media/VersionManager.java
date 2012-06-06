package org.sakaiproject.nakamura.media;


import org.sakaiproject.nakamura.api.lite.content.Content;
import org.sakaiproject.nakamura.api.lite.content.ContentManager;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;

import java.util.ArrayList;
import java.util.List;


class VersionManager {

  private ContentManager contentManager;


  public VersionManager(ContentManager cm) {
    this.contentManager = cm;
  }


  // Get the versions metadata for 'pid' from the most recent version to the
  // oldest.
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

      result.add(new Version(pid,
                             (String)current.getProperty("_id"),
                             (String)last.getProperty("sakai:pooled-content-file-name"),
                             (String)last.getProperty("sakai:description"),
                             (String)last.getProperty("sakai:fileextension"),
                             contentManager));
    }

    return result;
  }
}

