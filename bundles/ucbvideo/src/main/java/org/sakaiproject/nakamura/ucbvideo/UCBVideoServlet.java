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
package org.sakaiproject.nakamura.ucbvideo;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.FileRequestEntity;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.StringPart;
import org.apache.commons.lang.StringUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.felix.scr.annotations.sling.SlingServlet;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.apache.sling.commons.json.io.JSONWriter;
import org.apache.sling.commons.osgi.PropertiesUtil;
import org.osgi.service.component.ComponentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@Component(metatype = true, policy = ConfigurationPolicy.REQUIRE)
@Service
@Property(name = "alias", value = "/var/brightcove")
public class UCBVideoServlet extends HttpServlet {
  @Property
  public static final String READ_TOKEN = "readToken";

  @Property
  public static final String WRITE_TOKEN = "writeToken";

  @Property(value = "http://api.brightcove.com/services")
  public static final String BASE_URL = "baseUrl";

  private static final Logger LOG = LoggerFactory.getLogger(UCBVideoServlet.class);

  protected String readToken;
  protected String writeToken;
  protected String baseUrl;
  protected String libraryUrl;
  protected String postUrl;

  private HttpClient client;

  public UCBVideoServlet() {
    client = new HttpClient();
  }

  @Activate
  @Modified
  protected void activate(Map<?, ?> props) {
    readToken = PropertiesUtil.toString(props.get(READ_TOKEN), null);
    // require readToken
    if (StringUtils.isBlank(readToken)) {
      throw new ComponentException("'readToken' required to communicate with BrightCove");
    }

    writeToken = PropertiesUtil.toString(props.get(WRITE_TOKEN), null);
    // if writeToken is blank, use readToken
    if (StringUtils.isBlank(writeToken)) {
      throw new ComponentException("'writeToken' required to communicate with BrightCove");
    }

    baseUrl = PropertiesUtil.toString(props.get(BASE_URL), null);
    // require baseUrl
    if (StringUtils.isBlank(baseUrl)) {
      throw new ComponentException("'baseUrl' required to communicate with BrightCove");
    }
    libraryUrl = String.format("%s/library", baseUrl);
    postUrl = String.format("%s/post", baseUrl);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    PostMethod post = null;

    try {
      JSONObject json = new JSONObject()
          .put("method", "get_upload_status")
          .put("params", new JSONObject()
              .put("token", writeToken)
              .put("video_id", req.getParameter("video_id")));
      // Define the url to the api
      post = new PostMethod(postUrl);
      Part[] parts = { new StringPart("JSON-RPC", json.toString()) };
      post.setRequestEntity(new MultipartRequestEntity(parts, post.getParams()));
      int returnCode = client.executeMethod(post);

      String response = post.getResponseBodyAsString();
      String msg = String.format("Video upload information [%s]: %s", new Object[] {
          returnCode, response });
      LOG.info(msg);
      resp.getWriter().write(response);
    } catch (JSONException e) {
      LOG.error(e.getMessage(), e);
    } finally {
      if (post != null) {
        post.releaseConnection();
      }
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    /*
     * STEP 1. Handle the incoming request from the client
     */

    // Request parsing using the FileUpload lib from Jakarta Commons
    // http://commons.apache.org/fileupload/

    // Create a factory for disk-based file items
    DiskFileItemFactory factory = new DiskFileItemFactory();

    // Create a new file upload handler
    ServletFileUpload upload = new ServletFileUpload(factory);
    upload.setSizeMax(1000000000);

    PostMethod post = null;
    try {
      // Parse the request into a list of DiskFileItems
      @SuppressWarnings("unchecked")
      List<DiskFileItem> items = upload.parseRequest(req);

      File videoFile = items.get(0).getStoreLocation();
      String videoName = req.getParameter("name");
      String videoDescription = req.getParameter("desc");
      /*
       * STEP 2. Assemble the JSON params
       */
      JSONObject json = new JSONObject()
          .put("method", "create_video")
          .put("params", new JSONObject()
              .put("token", writeToken)
              .put("video", new JSONObject()
                  .put("name", videoName)
                  .put("shortDescription", videoDescription)));


      Part[] parts = {
          new StringPart("JSON-RPC", json.toString()),
          new FilePart(videoFile.getName(), videoFile)
      };

      post = new PostMethod(postUrl);
      post.setRequestEntity(new MultipartRequestEntity(parts, post.getParams()));
      int returnCode = client.executeMethod(post);

      String response = post.getResponseBodyAsString();
      String msg = String.format("Posted video information [%s]: %s", new Object[] {
          returnCode, response });
      LOG.info(msg);
      PrintWriter w = resp.getWriter();
      w.write("{'post':");
      w.write(json.toString());
      w.write(",\n'response':");
      w.write(response);
      w.write("}\n");
    } catch (FileUploadException e) {
      LOG.error(e.getMessage(), e);
    } catch (JSONException e) {
      LOG.error(e.getMessage(), e);
    } finally {
      if (post != null) {
        post.releaseConnection();
      }
    }
  }

  @Override
  protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    super.doDelete(req, resp);
  }
}
