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
 * specific language governing permissions and limitations
 * under the License.
 */
package org.sakaiproject.nakamura.media;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.sakaiproject.nakamura.api.media.MediaService;
import org.sakaiproject.nakamura.api.media.MediaServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@Component(metatype = true, policy = ConfigurationPolicy.REQUIRE)
@Service
@Property(name = "alias", value = "/var/brightcove")
public class MediaServlet extends HttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(MediaServlet.class);

  @Reference
  MediaService mediaService;

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    try {
      String status = mediaService.getStatus(req.getParameter("media_id"));
      resp.getWriter().write(status);
    } catch (MediaServiceException e) {
      throw new ServletException(e.getMessage(), e);
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
    int sizeMax = 1024 /*1 kB*/ * 1000 /*1 MB*/ * 100 /*100 MB*/; 
    upload.setSizeMax(sizeMax);

    try {
      // Parse the request into a list of DiskFileItems
      @SuppressWarnings("unchecked")
      List<DiskFileItem> items = upload.parseRequest(req);

      File mediaFile = items.get(0).getStoreLocation();
      String mediaName = req.getParameter("name");
      String mediaDescription = req.getParameter("desc");
      String[] tags = req.getParameterValues("tags");
      /*
       * STEP 2. Assemble the JSON params
       */
      FileInputStream fis = new FileInputStream(mediaFile);
      String response = null;
      try {
        response = mediaService.createMedia(fis, mediaName, mediaDescription,
                                            tags);
      } finally {
        fis.close();
      }

      String msg = "Posted media information: " + response;
      LOG.info(msg);
      resp.getWriter().write(msg);
    } catch (FileUploadException e) {
      throw new ServletException(e.getMessage(), e);
    } catch (MediaServiceException e) {
      throw new ServletException(e.getMessage(), e);
    }
  }

  @Override
  protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    super.doDelete(req, resp);
  }
}
