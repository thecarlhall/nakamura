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
package org.sakaiproject.nakamura.media.brightcove;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.HashSet;

import org.apache.commons.io.IOUtils;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.PartSource;
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
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.apache.sling.commons.osgi.PropertiesUtil;
import org.osgi.service.component.ComponentException;
import org.sakaiproject.nakamura.api.media.MediaService;
import org.sakaiproject.nakamura.api.media.MediaStatus;
import org.sakaiproject.nakamura.api.media.MediaServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(metatype = true, policy = ConfigurationPolicy.REQUIRE)
@Service
public class BrightCoveMediaService implements MediaService {

  static final String SCRIPT_SRC_DEFAULT = "http://admin.brightcove.com/js/BrightcoveExperiences.js";
  @Property(value = SCRIPT_SRC_DEFAULT)
  public static final String SCRIPT_SRC = "script.src";

  static final String OBJECT_CLASS_DEFAULT = "BrightcoveExperience";
  @Property(value = OBJECT_CLASS_DEFAULT)
  public static final String OBJECT_CLASS = "myExperience.class";

  static final String BG_COLOR_DEFAULT = "#FFFFFF";
  @Property
  public static final String BG_COLOR = "bgcolor";

  static final String WIDTH_DEFAULT = "500";
  @Property
  public static final String WIDTH = "width";

  static final String HEIGHT_DEFAULT = "470";
  @Property
  public static final String HEIGHT = "height";

  @Property
  public static final String PLAYER_ID = "playerID";

  @Property
  public static final String PLAYER_KEY = "playerKey";

  static final boolean IS_VID_DEFAULT = true;
  @Property(boolValue = IS_VID_DEFAULT)
  public static final String IS_VID = "isVid";

  static final boolean DYNAMIC_STREAMING_DEFAULT = true;
  @Property(boolValue = DYNAMIC_STREAMING_DEFAULT)
  public static final String DYNAMIC_STREAMING = "dynamicStreaming";

  static final String W_MODE_DEFAULT = "opaque";
  @Property
  public static final String W_MODE = "wmode";

  private static final Logger LOG = LoggerFactory.getLogger(BrightCoveMediaService.class);

  @Property
  public static final String READ_TOKEN = "readToken";

  @Property
  public static final String WRITE_TOKEN = "writeToken";

  static final String BASE_URL_DEFAULT = "http://api.brightcove.com/services";
  @Property(value = BASE_URL_DEFAULT)
  public static final String BASE_URL = "baseUrl";

  @Property(value = { "3gp", "3g2", "h261", "h263", "h264", "jpgv", "jpm", "jpgm", "mj2", "mjp2", "mp4", "mp4v", "mpg4", "mpeg", "mpg", "mpe", "m1v", "m2v", "ogv", "qt", "mov", "uvh", "uvvh", "uvm", "uvvm", "uvp", "uvvp", "uvs", "uvvs", "uvv", "uvvv", "dvb", "fvt", "mxu", "m4u", "pyv", "uvu", "uvvu", "viv", "webm", "f4v", "fli", "flv", "m4v", "mkv", "mk3d", "mks", "mng", "asf", "asx", "vob", "wm", "wmv", "wmx", "wvx", "avi", "movie", "smv" })
  public static final String VIDEO_EXTENSIONS = "supportedVideoExtensionsList";

  private static final String REQ_MSG_TMPL = "'%s' required to communicate with BrightCove";

  private static final String OBJECT_EL_TMPL = "<script language=\"JavaScript\" type=\"text/javascript\" src=\"%s\"></script>" +
      "<object id=\"myExperience%s\" class=\"%s\">" +
      "  <param name=\"bgcolor\" value=\"%s\" />" +
      "  <param name=\"width\" value=\"%s\" />" +
      "  <param name=\"height\" value=\"%s\" />" +
      "  <param name=\"playerID\" value=\"%s\" />" +
      "  <param name=\"playerKey\" value=\"%s\" />" +
      "  <param name=\"isVid\" value=\"%s\" />" +
      "  <param name=\"dynamicStreaming\" value=\"%s\" />" +
      "  <param name=\"wmode\" value=\"%s\" />" +
      "  <param name=\"@videoPlayer\" value=\"%s\" />" +
      "</object>";


  String readToken;
  String writeToken;
  String supportedVideoExtensionsList;
  String baseUrl;
  String libraryUrl;
  String postUrl;
  String bgcolor;
  String dynamicStreaming;
  String height;
  String isVid;
  String objectClass;
  String playerID;
  String playerKey;
  String scriptSrc;
  String width;
  String wMode;

  private HashSet<String> supportedVideoExtensions = new HashSet<String>();

  private HttpClient client;

  public BrightCoveMediaService() {
    client = new HttpClient();
  }

  @Activate
  @Modified
  protected void activate(Map<?, ?> props) {
    // ---------- required properties ------------------------------------------
    readToken = PropertiesUtil.toString(props.get(READ_TOKEN), null);
    // require readToken
    if (StringUtils.isBlank(readToken)) {
      throw new ComponentException(String.format(REQ_MSG_TMPL, "readToken"));
    }

    writeToken = PropertiesUtil.toString(props.get(WRITE_TOKEN), null);
    // require writeToken
    if (StringUtils.isBlank(writeToken)) {
      throw new ComponentException(String.format(REQ_MSG_TMPL, "writeToken"));
    }

    playerID = PropertiesUtil.toString(props.get(PLAYER_ID), null);
    // require playerID
    if (StringUtils.isBlank(playerID)) {
      throw new ComponentException(String.format(REQ_MSG_TMPL, "playerID"));
    }

    playerKey = PropertiesUtil.toString(props.get(PLAYER_KEY), null);
    // require playerKey
    if (StringUtils.isBlank(playerKey)) {
      throw new ComponentException(String.format(REQ_MSG_TMPL, "playerKey"));
    }

    baseUrl = PropertiesUtil.toString(props.get(BASE_URL), BASE_URL_DEFAULT);
    // require baseUrl
    if (StringUtils.isBlank(baseUrl)) {
      throw new ComponentException(String.format(REQ_MSG_TMPL, "baseUrl"));
    }
    libraryUrl = String.format("%s/library", baseUrl);
    postUrl = String.format("%s/post", baseUrl);

    // ---------- optional properties ------------------------------------------
    for (String ext : PropertiesUtil.toStringArray(props.get(VIDEO_EXTENSIONS), new String[] {}))  {
      supportedVideoExtensions.add(ext.toLowerCase());
    }

    bgcolor = PropertiesUtil.toString(props.get(BG_COLOR), BG_COLOR_DEFAULT);
    dynamicStreaming = Boolean.toString(PropertiesUtil.toBoolean(props.get(DYNAMIC_STREAMING), DYNAMIC_STREAMING_DEFAULT));
    height = PropertiesUtil.toString(props.get(HEIGHT), HEIGHT_DEFAULT);
    isVid = Boolean.toString(PropertiesUtil.toBoolean(props.get(IS_VID), IS_VID_DEFAULT));
    objectClass = PropertiesUtil.toString(props.get(OBJECT_CLASS), OBJECT_CLASS_DEFAULT);
    scriptSrc = PropertiesUtil.toString(props.get(SCRIPT_SRC), SCRIPT_SRC_DEFAULT);
    width = PropertiesUtil.toString(WIDTH, WIDTH_DEFAULT);
    wMode = PropertiesUtil.toString(W_MODE, W_MODE_DEFAULT);
  }

  // --------------- MediaService interface -----------------------------------
  /**
   * {@inheritDoc}
   *
   * @see org.sakaiproject.nakamura.api.media.MediaService#createMedia(java.io.InputStream,
   *      java.lang.String, java.lang.String, java.lang.String[])
   */
  @Override
  public String createMedia(InputStream mediaFile, String title, String description,
      String extension, String[] tags) throws MediaServiceException {
    String response = sendMedia(title, description, extension, tags, mediaFile, null);
    LOG.info(response);
    return response;
  }

  /**
   * {@inheritDoc}
   *
   * @see org.sakaiproject.nakamura.api.media.MediaService#updateMedia(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String[])
   */
  @Override
  public String updateMedia(String id, String title, String description, String[] tags)
      throws MediaServiceException {
    String response = sendMedia(title, description, null, tags, null, id);
    LOG.info(response);
    return response;
  }

  /**
   * {@inheritDoc}
   *
   * @see org.sakaiproject.nakamura.api.media.MediaService#getStatus(java.lang.String)
   */
  @Override
  public MediaStatus getStatus(String id) throws MediaServiceException,
      IOException {
    PostMethod post = null;

    MediaStatus result;

    try {
      // TODO don't check for this every time. Should have a latency window and only check
      // for certain statuses
      JSONObject json = new JSONObject()
          .put("method", "get_upload_status")
          .put("params", new JSONObject()
              .put("token", writeToken)
              .put("video_id", id));
      // Define the url to the api
      post = new PostMethod(postUrl);
      Part[] parts = { new StringPart("JSON-RPC", json.toString()) };
      post.setRequestEntity(new MultipartRequestEntity(parts, post.getParams()));
      int returnCode = client.executeMethod(post);

      JSONObject response = new JSONObject(post.getResponseBodyAsString());
      final String status = response.getString("result");

      LOG.info("Status from Brightcove: {}", status);

      result = new MediaStatus() {
          public boolean isReady() {
            return ("COMPLETE".equals(status));
          }

          public boolean isProcessing() {
            return ("PROCESSING".equals(status) || "UPLOADING".equals(status));
          }

          public boolean isError() {
            return "ERROR".equals(status);
          }
        };

    } catch (JSONException e) {
      LOG.error(e.getMessage(), e);
      throw new MediaServiceException(e.getMessage(), e);
    } finally {
      if (post != null) {
        post.releaseConnection();
      }
    }

    return result;
  }

  /**
   * {@inheritDoc}
   *
   * @see org.sakaiproject.nakamura.api.media.MediaService#getPlayerFragment(java.lang.String)
   */
  @Override
  public String getPlayerFragment(String id) {
    return String.format(OBJECT_EL_TMPL, scriptSrc, id, objectClass, bgcolor,
        width, height, playerID, playerKey, isVid, dynamicStreaming, wMode, id);
  }


  /**
   * {@inheritDoc}
   *
   * @see org.sakaiproject.nakamura.api.media.MediaService#getMimeType()
   */
  @Override
  public String getMimeType() {
    return "application/x-media-brightcove";
  }


  /**
   * {@inheritDoc}
   *
   * @see org.sakaiproject.nakamura.api.media.MediaService#acceptsMimeType(String)
   */
  @Override
  public boolean acceptsFileType(String mimeType, String extension) {
    return mimeType.startsWith("video/") && supportedVideoExtensions.contains(extension);
  }


  private FileInputStream asFileInputStream(InputStream is) throws MediaServiceException {
    if (is instanceof FileInputStream) {
      // Easy.
      return (FileInputStream) is;
    }

    try {
      final File tmpfile = File.createTempFile("oae_media", null);

      FileOutputStream out = new FileOutputStream(tmpfile);
      IOUtils.copy(is, out);
      out.close();
      is.close();

      return new FileInputStream (tmpfile) {
        public void close() throws IOException {
          try {
            super.close();
          } finally {
            tmpfile.delete();
          }
        }
      };
    } catch (FileNotFoundException e) {
      throw new MediaServiceException("Failed to create temporary file", e); 
    } catch (IOException e) {
      throw new MediaServiceException("Failed to create temporary file", e); 
    }
  }

  private String sendMedia(final String title, String description, final String extension, String[] tags,
                           InputStream mediaFile, final String id) throws MediaServiceException {
    if (id == null && mediaFile == null) {
      throw new IllegalArgumentException("Must supply 'id' or 'mediaFile'");
    }

    PostMethod post = null;
    try {
      /*
       * Assemble the JSON params
       */
      JSONObject media = new JSONObject()
        .put("name", title)
        .put("shortDescription", description)
        .put("tags", Arrays.asList(tags));

      String method = "update_video";

      if (mediaFile != null) {
        method = "create_video";
      } else {
        media.put("id", id);
      }

      JSONObject json = new JSONObject()
          .put("method", method)
          .put("params", new JSONObject()
              .put("token", writeToken)
              .put("video", media));

      Part[] parts;
      if (mediaFile != null) {
        final FileInputStream fileInput = asFileInputStream(mediaFile);
        parts = new Part[] {
          new StringPart("JSON-RPC", json.toString()),
          new FilePart("fileData",
                       new PartSource () {
                         public InputStream createInputStream() {
                           return fileInput;
                         }

                         public String getFileName() {
                           if (extension != null) {
                             return title + "." + extension;
                           } else {
                             return title;
                           }
                         }

                         public long getLength() {
                           try {
                             return fileInput.getChannel().size();
                           } catch (IOException e) {
                             LOG.error("Failed to calculate size for file '{}'", id);
                             throw new RuntimeException("getLength failed for file: " + id, e); 
                           }
                         }
                       })
        };
      } else {
        parts = new Part[] { new StringPart("JSON-RPC", json.toString()) };
      }

      post = new PostMethod(postUrl);
      post.setRequestEntity(new MultipartRequestEntity(parts, post.getParams()));
      int returnCode = client.executeMethod(post);

      String response = post.getResponseBodyAsString();

      String msg = String.format("Sent: %s, Posted media information [%s]: %s", new Object[] {
          json.toString(), returnCode, response });
      LOG.info(msg);

      JSONObject responseJSON = new JSONObject(response);

      JSONObject error = responseJSON.has("error") ? responseJSON.optJSONObject("error") : null;

      if (error != null) {
          throw new MediaServiceException(error.getString("name") + ": " + error.getString("message"));
      }

      if (mediaFile != null) {
        // This was an upload.  Return the new ID
        return String.valueOf(responseJSON.getLong("result"));
      } else {
        // An update.  Return the old ID
        return id;
      }

    } catch (JSONException e) {
      throw new MediaServiceException(e.getMessage(), e);
    } catch (FileNotFoundException e) {
      throw new MediaServiceException(e.getMessage(), e);
    } catch (HttpException e) {
      throw new MediaServiceException(e.getMessage(), e);
    } catch (IOException e) {
      throw new MediaServiceException(e.getMessage(), e);
    } finally {
      if (post != null) {
        post.releaseConnection();
      }
    }
  }
}
