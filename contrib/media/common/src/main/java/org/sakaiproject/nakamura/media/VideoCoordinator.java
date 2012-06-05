package org.sakaiproject.nakamura.media;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Maps;

import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Queue;
import javax.jms.Message;
import javax.jms.JMSException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.sakaiproject.nakamura.api.lite.Repository;
import org.sakaiproject.nakamura.api.lite.content.Content;
import org.sakaiproject.nakamura.api.lite.content.ContentManager;
import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;

import org.sakaiproject.nakamura.api.files.FilesConstants;


class Slowdown
{
    private long delay;
    private long lastTime = -1;

    private static final Logger LOGGER = LoggerFactory
        .getLogger(Slowdown.class);


    public Slowdown(long delay)
    {
        this.delay = delay;
    }


    public void sleep()
    {
        long now = System.currentTimeMillis();

        if (lastTime > 0) {
            long elapsed = (now - lastTime);
            if (elapsed < delay) {
                try {
                    LOGGER.info("Waiting...");
                    Thread.sleep(delay - elapsed);
                } catch (InterruptedException e) {}
            }
        } else {
            lastTime = now;
        }
    }
}


class MediaNode
{
    private static final Logger LOGGER = LoggerFactory
        .getLogger(MediaNode.class);


    private ContentManager contentManager;
    private String path;

    protected MediaNode(ContentManager cm, String path)
    {
        this.contentManager = cm;
        this.path = path;
    }


    public static MediaNode get(String parent, ContentManager cm)
        throws StorageClientException, AccessDeniedException
    {
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


    public void recordVersion(Version version)
        throws AccessDeniedException, StorageClientException
    {
        Content replicationStatus = getReplicationStatus(version);

        replicationStatus.setProperty("bodyUploaded", "Y");
        replicationStatus.setProperty("metadataVersion",
                                      version.metadataVersion());

        contentManager.update(replicationStatus);

    }


    private Content getReplicationStatus(Version version)
        throws StorageClientException, AccessDeniedException
    {
        String mypath = path + "/replicationStatus/" + version.getVersionId();

        Content replicationStatus = contentManager.get(mypath);

        if (replicationStatus == null) {
            replicationStatus = new Content(mypath, new HashMap<String, Object>());
            contentManager.update(replicationStatus, true);
        }

        return replicationStatus;
    }


    public boolean isBodyUploaded(Version version)
        throws StorageClientException, AccessDeniedException
    {
        Content replicationStatus = getReplicationStatus(version);

        return ("Y".equals(replicationStatus.getProperty("bodyUploaded")));
    }


    public boolean isMetadataUpToDate(Version version)
        throws StorageClientException, AccessDeniedException
    {
        Content replicationStatus = getReplicationStatus(version);

        return (version.metadataVersion().equals(replicationStatus.getProperty("metadataVersion")));
    }
}


class Version
{
    private String pid;
    private String versionId;
    private String title;
    private String description;
    private ContentManager contentManager;


    public Version(String pid, String versionId, String title, String description,
                   ContentManager cm)
    {
        contentManager = cm;
        this.pid = pid;
        this.versionId = versionId;
        this.title = title;
        this.description = description;
    }


    public String getVersionId()
    {
        return versionId;
    }


    public String getTitle()
    {
        return title;
    }


    public String getDescription()
    {
        return description;
    }


    public InputStream getStoredContent()
        throws AccessDeniedException, StorageClientException, IOException
    {
        return contentManager.getVersionInputStream(pid, versionId);
    }


    public Long metadataVersion()
    {
        int hash = (title + description).hashCode();

        return new Long((long)hash);
    }


    public String toString()
    {
        return versionId + "@" + pid;
    }
}


class VersionManager
{
    private ContentManager contentManager;


    public VersionManager(ContentManager cm)
    {
        this.contentManager = cm;
    }


    // Get the versions metadata for 'pid' from the most recent version to the
    // oldest.
    public List<Version> getVersionsMetadata(String pid)
        throws StorageClientException, AccessDeniedException
    {
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
                                   contentManager));
        }

        return result;
    }
}


public class VideoCoordinator implements Runnable
{
    private static final Logger LOGGER = LoggerFactory
        .getLogger(VideoCoordinator.class);

    protected Repository sparseRepository;

    private int WORKER_COUNT = 5;
    private int POLL_FREQUENCY = 5000;

    private ConnectionFactory connectionFactory;
    private String queueName;
    private AtomicBoolean running;

    private Connection conn = null;
    private Session jmsSession = null;
    private MessageConsumer videoQueueConsumer = null;

    Thread activeThread;


    public VideoCoordinator(ConnectionFactory connectionFactory,
                               String queueName,
                               Repository sparseRepository)
    {
        this.connectionFactory = connectionFactory;
        this.queueName = queueName;
        this.sparseRepository = sparseRepository;

        running = new AtomicBoolean(false);
    }


    public void start()
    {
        running.set(true);

        activeThread = new Thread(this);
        activeThread.setName("VideoCoordinator thread");
        activeThread.start();
    }


    public void shutdown()
    {
        running.set(false);

        if (activeThread != null) {
            try {
                activeThread.interrupt();
                activeThread.join();
            } catch (InterruptedException e) {
                LOGGER.error("Caught InterruptedException on shutdown: {}", e);
            }
        } else {
            LOGGER.info("No thread active");
        }
    }


    private void connectToJMS(final LinkedBlockingQueue<Message> incoming)
    {
        try {
            conn = connectionFactory.createConnection();
            conn.start();

            jmsSession = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

            Queue videoQueue = jmsSession.createQueue(queueName);
            videoQueueConsumer = jmsSession.createConsumer(videoQueue);

            videoQueueConsumer.setMessageListener(new MessageListener() {
                    public void onMessage(Message msg)
                    {
                        incoming.add(msg);
                    }
                });

        } catch (JMSException e) {
            LOGGER.error("Fatal: Can't connect to JMS queue.  Aborting.");
            throw new RuntimeException(e.getMessage(), e);
        }
    }


    private void disconnectFromJMS()
    {
        try {
            videoQueueConsumer.close();
            jmsSession.close();
            conn.close();
        } catch (JMSException e) {
        }
    }


    private ExecutorService[] createWorkerPool()
    {
        ExecutorService[] pool = new ExecutorService[WORKER_COUNT];

        for (int i = 0; i < WORKER_COUNT; i++) {
            pool[i] = Executors.newFixedThreadPool(1);
        }

        return pool;
    }


    private boolean isVideo(String mimeType)
    {
        return (mimeType != null && mimeType.startsWith("video/"));
    }



    private void syncVideo(Content obj, ContentManager cm)
    {
        LOGGER.info("Processing video now...");

        String path = obj.getPath();

        try {
            MediaNode mediaNode = MediaNode.get(path, cm);
            VersionManager vm = new VersionManager(cm);

            for (Version version : vm.getVersionsMetadata(path)) {
                LOGGER.info("Processing version {} of object {}",
                            version, path);

                if (!mediaNode.isBodyUploaded(version)) {
                    LOGGER.info("Uploading body for version {} of object {}",
                                version, path);
                }

                if (!mediaNode.isMetadataUpToDate(version)) {
                    LOGGER.info("Updating metadata for version {} of object {}",
                                version, path);
                }

                mediaNode.recordVersion(version);
            }


            // Sync tags
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ie) {
            }

        } catch (StorageClientException e) {
            LOGGER.info("StorageClientException when syncing video: {}",
                        path);
            e.printStackTrace();
        } catch (AccessDeniedException e) {
            LOGGER.info("AccessDeniedException when syncing video: {}",
                        path);
            e.printStackTrace();
        }
    }


    private void processObject(String pid)
    {
        org.sakaiproject.nakamura.api.lite.Session sparseSession = null;
        try {
            sparseSession = sparseRepository.loginAdministrative();

            ContentManager contentManager = sparseSession.getContentManager();
            Content obj = contentManager.get(pid);

            if (obj == null) {
                LOGGER.warn("Object '{}' couldn't be fetched from sparse", pid);
                return;
            }

            String mimeType = (String)obj.getProperty(FilesConstants.POOLED_CONTENT_MIMETYPE);
            if (!isVideo(mimeType)) {
                LOGGER.info("Path '{}' isn't a video (type is: {}).  Skipped.",
                            pid, mimeType);
                return;
            }

            syncVideo(obj, contentManager);

        } catch (StorageClientException e) {
            LOGGER.warn("StorageClientException while processing {}: {}",
                        pid, e);
            e.printStackTrace();
        } catch (AccessDeniedException e) {
            LOGGER.warn("AccessDeniedException while processing {}: {}",
                        pid, e);
            e.printStackTrace();
        } finally {
            try {
                if (sparseSession != null) {
                    sparseSession.logout();
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to logout of administrative session {} ",
                            e.getMessage());
            }

        }
    }


    private void clearDuplicates(LinkedBlockingQueue<Message> incoming,
                                 String pid)
    {
        try {
            // A bit funny to wait like this, but if we give a new file upload a
            // few seconds we can usually handle the initial upload and all its
            // events in one shot.  Just a dumb optimisation.
            Thread.sleep(2000);
        } catch (InterruptedException e) {}

        for (Message msg : incoming) {
            try {
                if (pid.equals(msg.getStringProperty("pid")) &&
                    incoming.remove(msg)) {

                    // We're about to handle this message, so skip the others
                    LOGGER.info("Discarded duplicate message: {}", msg);
                    msg.acknowledge();
                }
            } catch (JMSException e) {
                LOGGER.warn("Got a JMSException while clearing duplicates: {}",
                            e);
                e.printStackTrace();
            }
        }
    }


    public void run()
    {
        LOGGER.info("Running VideoCoordinator");

        final LinkedBlockingQueue<Message> incoming = new LinkedBlockingQueue<Message>();
        final LinkedBlockingQueue<String> completed = new LinkedBlockingQueue<String>();

        connectToJMS(incoming);

        ExecutorService[] workers = createWorkerPool();
        Map<String, Message> inProgress = new HashMap<String, Message>();

        Slowdown slowdown = new Slowdown((long)(POLL_FREQUENCY));
        while (running.get()) {
            try {
                Message msg = null;
                try {
                    msg = incoming.poll(POLL_FREQUENCY, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {}

                if (!running.get()) {
                    break;
                }

                if (msg != null) {
                    final String jobId = msg.getJMSMessageID();
                    final String pid = msg.getStringProperty("pid");

                    clearDuplicates(incoming, pid);

                    inProgress.put(jobId, msg);

                    // The hashing here ensures that same PID must always runs
                    // on the same worker.  Workers will need to update the
                    // content object being synced, so this is our concurrency
                    // control.
                    ExecutorService worker = workers[Math.abs(pid.hashCode() % WORKER_COUNT)];

                    worker.execute(new Runnable() {
                            public void run() {
                                LOGGER.info("Worker processing " + pid);

                                processObject(pid);

                                completed.add(jobId);
                                LOGGER.info("Worker completed processing {}",
                                            pid);
                            }
                        });

                    LOGGER.info("Videos waiting to process: {}; " +
                                "Videos in progress: {}",
                                incoming.size(), inProgress.size());
                }

                // Remove objects marked as completed from the "in progress"
                // queue
                while (!completed.isEmpty()) {
                    String jobId = completed.poll();

                    Message completedMsg = inProgress.get(jobId);

                    if (completedMsg != null) {
                        LOGGER.info("Completed processing: {} (pid: {})",
                                    jobId,
                                    completedMsg.getStringProperty("pid"));

                        completedMsg.acknowledge();
                        inProgress.remove(jobId);
                    }
                }

            } catch (JMSException e) {
                LOGGER.error("JMS exception while waiting for message: {}", e);
                e.printStackTrace();
                LOGGER.error("Waiting {} ms before trying again",
                             POLL_FREQUENCY);
            }

            // Paranoia...
            slowdown.sleep();
        }

        LOGGER.info("Shutting down worker pool...");
        for (ExecutorService worker : workers) {
            worker.shutdownNow();
        }

        disconnectFromJMS();
    }
}
