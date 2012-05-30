package org.sakaiproject.nakamura.ucbvideo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sakaiproject.nakamura.api.ucbvideo.UCBVideoService;

import org.osgi.service.component.ComponentContext;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Reference;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.sakaiproject.nakamura.api.files.FileUploadHandler;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventHandler;

import org.sakaiproject.nakamura.api.activemq.ConnectionFactoryService;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Queue;
import javax.jms.MapMessage;
import javax.jms.JMSException;

@Component(immediate = true, metatype = true)
@Service({UCBVideoService.class, EventHandler.class, FileUploadHandler.class})
@Properties(value = {
    @Property(name = "event.topics", value = {
        "org/sakaiproject/nakamura/lite/content/UPDATED"
        })
    })
public class UCBVideoServiceImpl
    implements UCBVideoService, EventHandler, FileUploadHandler
{
    private static final Logger LOGGER = LoggerFactory
        .getLogger(UCBVideoServiceImpl.class);

    private String QUEUE_NAME = "VIDEO";

    @Reference
    private ConnectionFactoryService connectionFactoryService;
    private ConnectionFactory connectionFactory;

    private UCBVideoCoordinator ucbVideoCoordinator;


    public void activate(ComponentContext context)
    {
        LOGGER.info("Activating UCBVideo bundle");

        connectionFactory =
            connectionFactoryService.getDefaultPooledConnectionFactory();

        ucbVideoCoordinator = new UCBVideoCoordinator(connectionFactory,
                                                      QUEUE_NAME);
        ucbVideoCoordinator.start();
    }


    public void deactivate(ComponentContext context)
    {
        LOGGER.info("Deactivating UCBVideo bundle");

        if (ucbVideoCoordinator != null) {
            LOGGER.info("Shutting down UCBVideo coordinator thread...");
            ucbVideoCoordinator.shutdown();
            LOGGER.info("Done");
        }
    }


    public void contentUpdated(String pid)
    {
        LOGGER.info("Content updated: {}", pid);

        try {
            Connection conn = connectionFactory.createConnection();
            Session jmsSession = conn.createSession(false,
                                                    Session.AUTO_ACKNOWLEDGE);
            Queue videoQueue = jmsSession.createQueue(QUEUE_NAME);

            MessageProducer producer = jmsSession.createProducer(videoQueue);

            producer.send(UCBVideoUtils.message(jmsSession, "pid", pid));

            producer.close();
            jmsSession.close();
            conn.close();

        } catch (JMSException e) {
            LOGGER.error("Failed when adding content PID to JMS queue: {}",
                         e);
            e.printStackTrace();
        }
    }


    public void handleEvent(Event event)
    {
        LOGGER.info("Got event: {}", event);

        String path = (String)event.getProperty("path");
        String op = (String)event.getProperty("op");
        String resourceType = (String)event.getProperty("resourceType");

        // We're only interested in top-level content PIDs here.  Don't bother
        // us about /activity nodes or other children.
        if (path.indexOf("/") == -1 &&
            "update".equals(op) &&
            "sakai/pooled-content".equals(resourceType)) {

            contentUpdated(path);
        }
    }


    public void handleFile(Map<String, Object> results,
                           String poolId,
                           InputStream inputStream,
                           String userId,
                           boolean isNew)
        throws IOException
    {
        contentUpdated(poolId);
    }
}
