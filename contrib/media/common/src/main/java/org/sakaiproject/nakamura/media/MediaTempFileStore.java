package org.sakaiproject.nakamura.media;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class MediaTempFileStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(MediaTempFileStore.class); 

  File storeDirectory;
  Map<String, String> lastYieldedPaths;
  Map<String, Map<String, File>> knownVersions;

  public MediaTempFileStore(String tempDir) {
    this.storeDirectory = new File(tempDir, "oae-media-store");

    storeDirectory.mkdirs();

    lastYieldedPaths = new HashMap<String, String>();
    knownVersions = new HashMap<String, Map<String, File>>();
  }


  synchronized File storageFor(String path) throws IOException {
    File dir = new File(storeDirectory, path);

    dir.mkdirs();

    long version = System.currentTimeMillis();

    File f = new File(dir, String.format("%020d.tmp", version));
    f.createNewFile();
    return f;
  }


  public void store(InputStream inputStream, String path)
    throws FileNotFoundException, IOException {

    File f = storageFor(path);

    FileOutputStream out = new FileOutputStream(f);

    IOUtils.copy(inputStream, out);

    out.close();

    f.renameTo(new File(f.getPath().replaceAll("\\.tmp$", "")));
  }


  // When uploading a file to the remote media service, the media coordinator
  // might need to try a couple of times before success.  When they ask for the
  // file corresponding to a particular version, we need to make sure we always
  // give them back the same file.  So, we track which versions have been
  // assigned to which files.
  private File getExistingVersion(String path, String versionId) {
    if (knownVersions.get(path) == null) {
      knownVersions.put(path, new HashMap<String, File>());
    }

    return knownVersions.get(path).get(versionId);
  }


  private void recordExistingVersion(String path, String versionId, File f) {
    if (knownVersions.get(path) == null) {
      knownVersions.put(path, new HashMap<String, File>());
    }

    knownVersions.get(path).put(versionId, f);
  }


  synchronized public File getFile(String path, String versionId) {

    File fileForVersion = getExistingVersion(path, versionId);

    if (fileForVersion != null) {
      return fileForVersion;
    }

    // If we didn't find the path/versionId pair in our stored mapping, that
    // just means we're being asked for a version we've never been asked about
    // before.  We'll walk through all the temp files we have for this path and
    // associate the oldest with the requested version.

    File dir = new File(storeDirectory, path);

    if (!dir.exists()) {
      return null;
    }

    File[] files = dir.listFiles();

    Arrays.sort(files, new Comparator<File> () {
        public int compare(File f1, File f2) {
          return f1.getName().compareTo(f2.getName());
        }
      });

    String lastYieldedPath = lastYieldedPaths.get(path);

    for (File f : files) {
      if (!f.getName().matches("^[0-9]+$")) {
        continue;
      }

      if (lastYieldedPath == null || f.getName().compareTo(lastYieldedPath) > 0) {
        lastYieldedPaths.put(path, f.getName());

        recordExistingVersion(path, versionId, f);

        return f;
      }
    }

    return null;
  }
  

  synchronized void completed(String path) {
    for (String versionId : knownVersions.get(path).keySet()) {
      completed(path, versionId);
    }
  }


  synchronized void completed(String path, String versionId) {
    File file = getExistingVersion(path, versionId);

    if (file == null) {
      LOGGER.info("Couldn't find a file associated with version '{}' of path '{}'",
                  versionId, path);
      return;
    }

    LOGGER.info("Removing temp file associated with version '{}' of path '{}'",
                versionId, path);

    file.delete();

    File parent = file.getParentFile();

    if (parent.listFiles().length == 0) {
      LOGGER.info("Cleaning up directory for path '{}' (no more files)", path);

      // Clean up
      parent.delete();

      lastYieldedPaths.remove(parent.getName());
      knownVersions.get(path).remove(versionId);
    }
  }
}
