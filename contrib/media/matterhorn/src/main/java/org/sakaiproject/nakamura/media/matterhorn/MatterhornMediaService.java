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

import java.io.IOException;
import java.io.InputStream;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Service;
import org.sakaiproject.nakamura.api.media.MediaService;
import org.sakaiproject.nakamura.api.media.MediaServiceException;
import org.sakaiproject.nakamura.api.media.MediaStatus;

/**
 *
 */
@Component(enabled = false, metatype = true, policy = ConfigurationPolicy.REQUIRE)
@Service
public class MatterhornMediaService implements MediaService {
  @Override
  public String createMedia(InputStream media, String title, String description, String extension, String[] tags)
    throws MediaServiceException {
    return null;
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
