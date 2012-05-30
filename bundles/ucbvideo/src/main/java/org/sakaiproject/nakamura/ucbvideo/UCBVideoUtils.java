package org.sakaiproject.nakamura.ucbvideo;

import javax.jms.Message;
import javax.jms.MapMessage;
import javax.jms.Session;
import javax.jms.JMSException;

class UCBVideoUtils
{
    public static Message message(Session jmsSession, String ... props) throws JMSException
    {
        MapMessage msg = jmsSession.createMapMessage();

        for (int i = 0; i < props.length; i += 2) {
            msg.setStringProperty(props[i], props[i + 1]);
            msg.setString(props[i], props[i + 1]);
        }

        return msg;
    }
}
