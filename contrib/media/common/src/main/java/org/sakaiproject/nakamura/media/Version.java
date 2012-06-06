package org.sakaiproject.nakamura.media;

import org.sakaiproject.nakamura.api.lite.content.Content;
import org.sakaiproject.nakamura.api.lite.content.ContentManager;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;

import java.io.IOException;
import java.io.InputStream;

class Version {

  private String pid;
  private String versionId;
  private String title;
  private String description;
  private String extension;
  private String mediaId;
  private String[] tags;
  private ContentManager contentManager;


  public Version(String pid, String versionId,
                 String title, String description, String extension,
                 String[] tags,
                 ContentManager cm) {
    contentManager = cm;
    this.pid = pid;
    this.versionId = versionId;
    this.title = title;
    this.description = description;
    this.extension = extension;
    this.tags = tags;
  }


  public String getVersionId() {
    return versionId;
  }


  public String getTitle() {
    return title;
  }


  public String getDescription() {
    return description;
  }


  public String getExtension() {
    return extension;
  }


  public String[] getTags() {
    return tags;
  }


  public InputStream getStoredContent() throws AccessDeniedException, StorageClientException, IOException {
    return contentManager.getVersionInputStream(pid, versionId);
  }


  public Long metadataVersion() {
    StringBuilder sb = new StringBuilder();

    sb.append(title);
    sb.append("\1");
    sb.append(description);
    sb.append("\1");

    for (String tag : tags) {
      sb.append(tag);
      sb.append("\1");
    }

    return new Long((long)(sb.toString().hashCode()));
  }


  public String toString() {
    return versionId + "@" + pid;
  }
}
