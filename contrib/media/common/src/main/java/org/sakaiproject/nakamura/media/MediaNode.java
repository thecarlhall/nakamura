package org.sakaiproject.nakamura.media;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;

import org.sakaiproject.nakamura.api.lite.content.Content;
import org.sakaiproject.nakamura.api.lite.content.ContentManager;


class MediaNode {

  private static final Logger LOGGER = LoggerFactory
    .getLogger(MediaNode.class);


  private ContentManager contentManager;
  private String path;

  protected MediaNode(ContentManager cm, String path) {
    this.contentManager = cm;
    this.path = path;
  }


  public static MediaNode get(String parent, ContentManager cm)
    throws StorageClientException, AccessDeniedException {
    String mediaNodePath = parent + "/medianode";

    Content obj = cm.get(mediaNodePath);

    if (obj == null) {
      obj = new Content(mediaNodePath, new HashMap<String, Object>());
      cm.update(obj, true);

      cm.update(new Content(mediaNodePath + "/replicationStatus",
                            new HashMap<String, Object>()),
                true);
    }

    return new MediaNode(cm, mediaNodePath);
  }


  public void recordVersion(Version version) throws AccessDeniedException, StorageClientException {

    Content replicationStatus = getReplicationStatus(version);

    replicationStatus.setProperty("bodyUploaded", "Y");
    replicationStatus.setProperty("metadataVersion",
                                  version.metadataVersion());

    contentManager.update(replicationStatus);

  }


  private Content getReplicationStatus(Version version) throws StorageClientException, AccessDeniedException {
    String mypath = path + "/replicationStatus/" + version.getVersionId();

    Content replicationStatus = contentManager.get(mypath);

    if (replicationStatus == null) {
      replicationStatus = new Content(mypath, new HashMap<String, Object>());
      contentManager.update(replicationStatus, true);
    }

    return replicationStatus;
  }


  public boolean isBodyUploaded(Version version) throws StorageClientException, AccessDeniedException {
    Content replicationStatus = getReplicationStatus(version);

    return ("Y".equals(replicationStatus.getProperty("bodyUploaded")));
  }


  public boolean isMetadataUpToDate(Version version) throws StorageClientException, AccessDeniedException {
    Content replicationStatus = getReplicationStatus(version);

    return (version.metadataVersion().equals(replicationStatus.getProperty("metadataVersion")));
  }
}
