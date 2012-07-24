/*
 * Licensed to the Sakai Foundation (SF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The SF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.sakaiproject.nakamura.media.matterhorn;

import java.io.File;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.auth.AuthPolicy;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.PartSource;
import org.apache.commons.httpclient.methods.multipart.StringPart;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Service;
import org.sakaiproject.nakamura.api.media.MediaService;
import org.sakaiproject.nakamura.api.media.MediaServiceException;
import org.sakaiproject.nakamura.api.media.MediaStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;
import javax.xml.xpath.XPathExpressionException;


/**
 *
 */
@Component(enabled = true, metatype = true, policy = ConfigurationPolicy.REQUIRE)
@Service
public class MatterhornMediaService implements MediaService {

  private static final Logger LOG = LoggerFactory.getLogger(MatterhornMediaService.class);

  private String matterhornUser = "matterhorn_system_account";
  private String matterhornPassword = "CHANGE_ME";
  private String matterhornUrl = "http://localhost:7080";



  private void matterhornAuthenticate(HttpClient client) {

    List authPrefs = new ArrayList(1);
    authPrefs.add(AuthPolicy.DIGEST);
    client.getParams().setParameter(AuthPolicy.AUTH_SCHEME_PRIORITY, authPrefs);

    AuthScope scope = new AuthScope(AuthScope.ANY_HOST,
        AuthScope.ANY_PORT);

    UsernamePasswordCredentials credentials =
      new UsernamePasswordCredentials(matterhornUser, matterhornPassword);

    client.getState().setCredentials(scope, credentials);
  }
    

  private String extractMediaId(String response) throws MediaServiceException {
    try {
    XPath xpath = XPathFactory.newInstance().newXPath();

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setNamespaceAware(true);
    dbf.setAttribute("http://xml.org/sax/features/namespaces", Boolean.TRUE);

    Document doc = dbf.newDocumentBuilder().parse(new ByteArrayInputStream(response.getBytes()));

    return ((Element) xpath.evaluate("//media/track", doc, XPathConstants.NODE)).getAttribute("id");
    } catch (ParserConfigurationException e) {
      throw new MediaServiceException("Failed to extract media ID from response: " + e);
    } catch (SAXException e) {
      throw new MediaServiceException("Failed to extract media ID from response: " + e);
    } catch (XPathExpressionException e) {
      throw new MediaServiceException("Failed to extract media ID from response: " + e);
    } catch (IOException e) {
      throw new MediaServiceException("Failed to extract media ID from response: " + e);
    }
  }


  @Override
  public String createMedia(final File mediaFile, final String title, final String description, final String extension, String[] tags)
    throws MediaServiceException {

    PostMethod post = new PostMethod(matterhornUrl + "/ingest/addMediaPackage");
    post.addRequestHeader(new Header("X-Requested-Auth", "Digest"));


    try {
      HttpClient client = new HttpClient();
      matterhornAuthenticate(client);

      Part[] parts;
      parts = new Part[] {
        new StringPart("flavor", "presenter/source"),
        new StringPart("title", title),
        new StringPart("creator", ""),
        new FilePart("track",
            new PartSource () {
              public InputStream createInputStream() {
                try {
                  return new FileInputStream(mediaFile);
                } catch (FileNotFoundException e) {
                  throw new RuntimeException("Couldn't find media file: " + mediaFile);
                }
              }

              public String getFileName() {
                if (extension != null) {
                  return title + "." + extension;
                } else {
                  return title;
                }
              }

              public long getLength() {
                return mediaFile.length();
              }
            })
      };

      post.setRequestEntity(new MultipartRequestEntity(parts, post.getParams()));


      int returnCode = client.executeMethod(post);
      String response = post.getResponseBodyAsString();

      if (returnCode != 200) {
        throw new MediaServiceException("Failure when processing media:" + response);
      }

      return extractMediaId(response);

    } catch (IOException e) {
      LOG.error("IOException while processing media: {}", e);
      e.printStackTrace();

      throw new MediaServiceException("IOException while processing media:" + e);
    } finally {
      if (post != null) {
        post.releaseConnection();
      }
    }
  }


  @Override
  public String updateMedia(String id, String title, String description, String[] tags)
      throws MediaServiceException {
    return null;
  }

  /**
   * {@inheritDoc}
   *
   * @see org.sakaiproject.nakamura.api.media.MediaService#getMimeType()
   */
  @Override
  public String getMimeType() {
    return "application/x-media-matterhorn";
  }


  /**
   * {@inheritDoc}
   *
   * @see org.sakaiproject.nakamura.api.media.MediaService#acceptsMimeType(String)
   */
  @Override
  public boolean acceptsFileType(String mimeType, String extension) {
    return mimeType.startsWith("video/");
  }


  @Override
  public String getPlayerFragment(String id) {
    return "IMPLEMENTME";
  }

  @Override
  public MediaStatus getStatus(String id) throws MediaServiceException, IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void deleteMedia(String id) throws MediaServiceException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public String[] getPlayerJSUrls(String id) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getPlayerInitJS(String id) {
    // TODO Auto-generated method stub
    return null;
  }

}
