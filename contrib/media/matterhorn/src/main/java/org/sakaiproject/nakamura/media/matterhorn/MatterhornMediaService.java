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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthChallengeParser;
import org.apache.commons.httpclient.auth.AuthPolicy;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.auth.AuthenticationException;
import org.apache.commons.httpclient.auth.DigestScheme;
import org.apache.commons.httpclient.auth.MalformedChallengeException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.PartSource;
import org.apache.commons.httpclient.methods.multipart.StringPart;
import org.apache.commons.lang.StringUtils;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Service;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.sakaiproject.nakamura.api.media.MediaService;
import org.sakaiproject.nakamura.api.media.MediaServiceException;
import org.sakaiproject.nakamura.api.media.MediaStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;


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
    

  private Document parseXML(String s) throws MediaServiceException {
    try {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setNamespaceAware(true);
    dbf.setAttribute("http://xml.org/sax/features/namespaces", Boolean.TRUE);

    return dbf.newDocumentBuilder().parse(new ByteArrayInputStream(s.getBytes("UTF-8")));
    } catch (ParserConfigurationException e) {
      throw new MediaServiceException("Failed to parse XML: " + e);
    } catch (SAXException e) {
      throw new MediaServiceException("Failed to parse XML: " + e);
    } catch (IOException e) {
      throw new MediaServiceException("Failed to parse XML: " + e);
    }
  }


  private String docToString(Node doc) throws MediaServiceException {
    try {
      Transformer t = TransformerFactory.newInstance().newTransformer();
      StringWriter sw = new StringWriter();

      t.transform(new DOMSource(doc), new StreamResult(sw));

      return sw.toString();
    } catch (TransformerException e) {
      throw new MediaServiceException("Failed to convert XML: " +e);
    }
  }


  private String extractMediaId(String response) throws MediaServiceException {
    try {
    XPath xpath = XPathFactory.newInstance().newXPath();

    Document doc = parseXML(response);

    String workflowId = ((Element)xpath.evaluate("//*[local-name() = 'workflow']", doc, XPathConstants.NODE)).getAttribute("id");
    String mediaPackageId = ((Element)xpath.evaluate("//mediapackage", doc, XPathConstants.NODE)).getAttribute("id");
    String trackId = ((Element)xpath.evaluate("//media/track", doc, XPathConstants.NODE)).getAttribute("id");
    String metadataId = ((Element)xpath.evaluate("//metadata/catalog", doc, XPathConstants.NODE)).getAttribute("id");
    
    JSONObject json = new JSONObject()
      .put("workflowId", workflowId)
      .put("mediaPackageId", mediaPackageId)
      .put("trackId", trackId)
      .put("metadataId", metadataId);

    return json.toString();

    } catch (JSONException e) {
      throw new MediaServiceException("Failed to extract media ID from response: " + e);
    } catch (XPathExpressionException e) {
      throw new MediaServiceException("Failed to extract media ID from response: " + e);
    }
  }


  // We send the auth request in advance of the real POST to avoid having to
  // POST our (potentially large) video file twice.  Basically, get the auth
  // challenge by sending an empty request and then only send our real payload
  // when we know auth will succeed.
  private String preAuthenticateRequest(HttpClient client, HttpMethodBase authRequest)
    throws IOException, MalformedChallengeException, AuthenticationException{
    authRequest.addRequestHeader(new Header("X-Requested-Auth", "Digest"));
    authRequest.setDoAuthentication(false);

    if (client.executeMethod(authRequest) == 401) {
      DigestScheme scheme = new DigestScheme();
      Map challenges = AuthChallengeParser.parseChallenges(authRequest.getResponseHeaders("WWW-Authenticate"));
      String challenge = (String)challenges.values().toArray()[0];

      scheme.processChallenge(challenge);

      UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(matterhornUser, matterhornPassword);
      return scheme.authenticate(credentials, authRequest);
    }

    return null;
  }


  private PostMethod digestAuthedPostMethod(HttpClient client, String uri) throws IOException, MalformedChallengeException, AuthenticationException {
    PostMethod authRequest = new PostMethod(matterhornUrl + uri);
    String auth = preAuthenticateRequest(client, authRequest);

    if (auth != null) {
      PostMethod request = new PostMethod(matterhornUrl + uri);
      request.addRequestHeader(new Header("Authorization", auth));
      request.setDoAuthentication(false);

      return request;
    }

    return null;
  }


  private GetMethod digestAuthedGetMethod(HttpClient client, String uri) throws IOException, MalformedChallengeException, AuthenticationException {
    GetMethod authRequest = new GetMethod(matterhornUrl + uri);
    String auth = preAuthenticateRequest(client, authRequest);

    if (auth != null) {
      GetMethod request = new GetMethod(matterhornUrl + uri);
      request.addRequestHeader(new Header("Authorization", auth));
      request.setDoAuthentication(false);

      return request;
    }

    return null;
  }



  @Override
  public String createMedia(final File mediaFile, final String title, final String description, final String extension, String[] tags)
    throws MediaServiceException {

    LOG.info("Processing media for {}", title);

    PostMethod post = null;

    try {
      HttpClient client = new HttpClient();

      post = digestAuthedPostMethod(client, "/ingest/addMediaPackage");

      Part[] parts = new Part[] {
        new StringPart("flavor", "presenter/source"),
        new StringPart("contributor", "Sakai OAE"),
        new StringPart("title", title),
        new StringPart("description", description),
        new StringPart("subject", StringUtils.join(tags, ", ")),
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


  private void updateMetadata(JSONObject ids, HttpClient client,
      String title, String description, String[] tags)
    throws IOException, XPathExpressionException, JSONException, MediaServiceException {

    PostMethod post = null;
    GetMethod get = null;
    try {
      // Get the current metadata
      String dcUrl = String.format("/files/mediapackage/%s/%s/dublincore.xml",
          ids.getString("mediaPackageId"),
          ids.getString("metadataId"));

      get = digestAuthedGetMethod(client, dcUrl);

      int returnCode = client.executeMethod(get);
      Document metadata = parseXML(get.getResponseBodyAsString());

      XPath xpath = XPathFactory.newInstance().newXPath();

      Element titleElt = (Element)xpath.evaluate("//*[local-name() = 'title']", metadata, XPathConstants.NODE);
      Element descriptionElt = (Element)xpath.evaluate("//*[local-name() = 'description']", metadata, XPathConstants.NODE);
      Element subjectElt = (Element)xpath.evaluate("//*[local-name() = 'subject']", metadata, XPathConstants.NODE);

      titleElt.setTextContent(title);
      descriptionElt.setTextContent(description);
      subjectElt.setTextContent(StringUtils.join(tags, ", "));

      final String xml = docToString(metadata);

      // Send the updated version back
      post = digestAuthedPostMethod(client, dcUrl);

      Part[] parts = new Part[] {
        new FilePart("content",
            new PartSource () {
              public InputStream createInputStream() {
                return new ByteArrayInputStream(xml.getBytes());
              }

              public String getFileName() {
                return "dublincore.xml";
              }

              public long getLength() {
                return xml.length();
              }
            })
      };

      post.setRequestEntity(new MultipartRequestEntity(parts, post.getParams()));

      returnCode = client.executeMethod(post);
      String response = post.getResponseBodyAsString();

      if ((returnCode / 100) != 2) {
        throw new MediaServiceException("Failure when processing media:" + response);
      }

    } finally {
      if (post != null) {
        post.releaseConnection();
      }
      if (get != null) {
        get.releaseConnection();
      }
    }
  }


  // For some reason empty 'tags' fields cause the player's JavaScript to choke.
  // Removing them entirely doesn't fix it, so setting them back to the defaults
  // here.
  //
  // I know, I know: I should have written something to walk the XML tree
  // instead of post-processing the string.  But I didn't, so there.  Maybe later.
  private String fixTagsHack(String xml)
  {
    return xml.replaceAll("<tags ?/>", "<tags><tag>engage</tag><tag>publish</tag></tags>");
  }


  private void refreshIndexes(JSONObject ids, HttpClient client)
    throws JSONException, IOException, XPathExpressionException, MediaServiceException {

    PostMethod post = null;
    GetMethod get = null;

    try {
      // Get the workflow
      String workflowUrl = String.format("/workflow/instance/%s.xml",
          ids.getString("workflowId"));

      get = digestAuthedGetMethod(client, workflowUrl);

      int returnCode = client.executeMethod(get);

      // Extract the media package
      Document workflow = parseXML(get.getResponseBodyAsString());
      XPath xpath = XPathFactory.newInstance().newXPath();
      Element mediaPackageElt = (Element)xpath.evaluate("//*[local-name() = 'mediapackage']", workflow, XPathConstants.NODE);
      String mediaPackageXML = fixTagsHack(docToString(mediaPackageElt));

      for (String toUpdate : new String[] { "episode", "search" }) {
        post = digestAuthedPostMethod(client, "/" + toUpdate + "/add");

        Part[] parts = new Part[] {
          new StringPart("mediapackage", mediaPackageXML)
        };

        post.setRequestEntity(new MultipartRequestEntity(parts, post.getParams()));

        returnCode = client.executeMethod(post);
        String response = post.getResponseBodyAsString();

        if ((returnCode / 100) != 2) {
          throw new MediaServiceException("Failure when processing media:" + response);
        }
      }
    } finally {
      if (post != null) {
        post.releaseConnection();
      }
      if (get != null) {
        get.releaseConnection();
      }
    }
  }



  @Override
  public String updateMedia(String id, String title, String description, String[] tags)
    throws MediaServiceException {

    try {
      JSONObject ids = new JSONObject(id);


      HttpClient client = new HttpClient();

      // Get the Dublin Core metadata file
      updateMetadata(ids, client, title, description, tags);
      refreshIndexes(ids, client);


      return id;

    } catch (IOException e) {
      throw new MediaServiceException("Failed to update media: " + e);
    } catch (XPathExpressionException e) {
      throw new MediaServiceException("Failed to update media: " + e);
    } catch (JSONException e) {
      throw new MediaServiceException("Failed to parse media ID: " + e);
    }
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
