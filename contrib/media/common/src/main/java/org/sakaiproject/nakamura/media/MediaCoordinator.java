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


public class MediaCoordinator implements Runnable {
  private static final Logger LOGGER = LoggerFactory
    .getLogger(MediaCoordinator.class);

  protected Repository sparseRepository;

  private int RETRY_MS = 5 * 60 * 1000;
  private int WORKER_COUNT = 5;
  private int POLL_FREQUENCY = 5000;

  private ConnectionFactory connectionFactory;
  private String queueName;
  private AtomicBoolean running;

  private Connection conn = null;
  private Session jmsSession = null;
  private MessageConsumer mediaQueueConsumer = null;

  Thread activeThread;


  public MediaCoordinator(ConnectionFactory connectionFactory, String queueName, Repository sparseRepository) {
    this.connectionFactory = connectionFactory;
    this.queueName = queueName;
    this.sparseRepository = sparseRepository;

    running = new AtomicBoolean(false);
  }


  public void start() {
    running.set(true);

    activeThread = new Thread(this);
    activeThread.setName("MediaCoordinator thread");
    activeThread.start();
  }


  public void shutdown() {
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


  private void connectToJMS(final LinkedBlockingQueue<Message> incoming) {
    try {
      conn = connectionFactory.createConnection();
      conn.start();

      jmsSession = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

      Queue mediaQueue = jmsSession.createQueue(queueName);
      mediaQueueConsumer = jmsSession.createConsumer(mediaQueue);

      mediaQueueConsumer.setMessageListener(new MessageListener() {
          public void onMessage(Message msg) {
            incoming.add(msg);
          }
        });

    } catch (JMSException e) {
      LOGGER.error("Fatal: Can't connect to JMS queue.  Aborting.");
      throw new RuntimeException(e.getMessage(), e);
    }
  }


  private void disconnectFromJMS() {
    try {
      mediaQueueConsumer.close();
      jmsSession.close();
      conn.close();
    } catch (JMSException e) {
    }
  }


  private ExecutorService[] createWorkerPool() {
    ExecutorService[] pool = new ExecutorService[WORKER_COUNT];

    for (int i = 0; i < WORKER_COUNT; i++) {
      pool[i] = Executors.newFixedThreadPool(1);
    }

    return pool;
  }


  private boolean isMedia(String mimeType) {
    return (mimeType != null && mimeType.startsWith("video/"));
  }



  private void syncMedia(Content obj, ContentManager cm) {
    LOGGER.info("Processing media now...");

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
      LOGGER.info("StorageClientException when syncing media: {}",
                  path);
      e.printStackTrace();
    } catch (AccessDeniedException e) {
      LOGGER.info("AccessDeniedException when syncing media: {}",
                  path);
      e.printStackTrace();
    }
  }


  private void processObject(String pid) {
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
      if (!isMedia(mimeType)) {
        LOGGER.info("Path '{}' isn't a media (type is: {}).  Skipped.",
                    pid, mimeType);
        return;
      }

      syncMedia(obj, contentManager);

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


  private void clearDuplicates(LinkedBlockingQueue<Message> incoming, String pid) {
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


  private class FailedJob
  {
    public String jobId;
    public long time;

    public FailedJob(String jobId, long time) {
      this.jobId = jobId;
      this.time = time;
    }
  }


  public void run() {
    LOGGER.info("Running MediaCoordinator");

    final LinkedBlockingQueue<Message> incoming = new LinkedBlockingQueue<Message>();
    final LinkedBlockingQueue<String> completed = new LinkedBlockingQueue<String>();
    final LinkedBlockingQueue<FailedJob> failed = new LinkedBlockingQueue<FailedJob>();

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

                try {
                  processObject(pid);

                  completed.add(jobId);
                  LOGGER.info("Worker completed processing {}",
                              pid);

                } catch (Exception e) {
                  LOGGER.warn("Failed while processing PID '{}'", pid);
                  e.printStackTrace();
                  LOGGER.warn("This job will be queued for retry in {} ms",
                              RETRY_MS);

                  failed.add(new FailedJob(jobId, System.currentTimeMillis()));
                }
              }
            });

          LOGGER.info("Media waiting to process: {}; " +
                      "Media in progress: {}",
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


        // Requeue jobs in the failed queue if their retry time has elapsed
        if (!failed.isEmpty()) {
          try {
            while (true) {
              FailedJob job = failed.peek();

              if (job != null && (System.currentTimeMillis() - job.time) > RETRY_MS) {
                job = failed.take();
                Message failedMsg = inProgress.get(job.jobId);

                if (failedMsg != null) {
                  LOGGER.info("Requeueing job for pid '{}'",
                              failedMsg.getStringProperty("pid"));

                  inProgress.remove(job.jobId);
                  
                  incoming.add(failedMsg);
                }
              } else {
                break;
              }
            }
          } catch (InterruptedException ex) {
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
