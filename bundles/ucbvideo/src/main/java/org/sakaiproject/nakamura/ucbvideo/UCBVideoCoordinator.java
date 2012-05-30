package org.sakaiproject.nakamura.ucbvideo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.LinkedBlockingQueue;

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


public class UCBVideoCoordinator implements Runnable
{
    private static final Logger LOGGER = LoggerFactory
        .getLogger(UCBVideoCoordinator.class);

    private int WORKER_COUNT = 5;
    private int POLL_FREQUENCY = 5000;

    private ConnectionFactory connectionFactory;
    private String queueName;
    private AtomicBoolean running;

    private Connection conn = null;
    private Session jmsSession = null;
    private MessageConsumer videoQueueConsumer = null;

    Thread activeThread;


    public UCBVideoCoordinator(ConnectionFactory connectionFactory,
                               String queueName)
    {
        this.connectionFactory = connectionFactory;
        this.queueName = queueName;

        running = new AtomicBoolean(false);
    }


    public void start()
    {
        running.set(true);

        activeThread = new Thread(this);
        activeThread.setName("UCBVideoCoordinator thread");
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


    public void run()
    {
        LOGGER.info("Running UCBVideoCoordinator");

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
                                    // Talk to BC/BH/LL here...
                                    Thread.sleep(10000);
                                } catch (InterruptedException ie) {}

                                completed.add(jobId);
                                LOGGER.info("Worker completed processing {}",
                                            pid);
                            }
                        });
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
