package org.sakaiproject.nakamura.media;

import javax.jms.Message;
import javax.jms.MapMessage;
import javax.jms.Session;
import javax.jms.JMSException;

import java.util.Map;
import java.util.HashMap;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

class MediaUtils
{
  private static Map<String,String> mimeTypeToExtensionMap;

  static {
    mimeTypeToExtensionMap = parseMimeTypes();
  }

  private static Map<String,String> parseMimeTypes() {
    InputStream in = MediaUtils.class.getResourceAsStream("/mime.types");

    if (in == null) {
      throw new RuntimeException("Couldn't find resource 'mime.types'.");
    }

    Map<String,String> result = new HashMap<String,String>();

    try {
      BufferedReader rdr = new BufferedReader(new InputStreamReader(in));
      try {
        String line;
        while ((line = rdr.readLine()) != null) {
          if (line.startsWith("#")) {
            continue;
          }

          String[] bits = line.split("\\s+");
          result.put(bits[0], bits[1]);
        }
      } finally {
        rdr.close();
      }

      return result;
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }


  public static String mimeTypeToExtension(String mimeType) {
    return mimeTypeToExtensionMap.get(mimeType);
  }


  public static Message message(Session jmsSession, String ... props) throws JMSException {
    MapMessage msg = jmsSession.createMapMessage();

    for (int i = 0; i < props.length; i += 2) {
      msg.setStringProperty(props[i], props[i + 1]);
      msg.setString(props[i], props[i + 1]);
    }

    return msg;
  }
}
