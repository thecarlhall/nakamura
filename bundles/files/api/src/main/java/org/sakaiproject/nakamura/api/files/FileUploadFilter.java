/**
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
package org.sakaiproject.nakamura.api.files;

import java.io.InputStream;
import org.apache.sling.api.request.RequestParameter;


public interface FileUploadFilter {
  /**
   * This method is called when a file is uploaded via the
   * CreateContentPoolServlet--before the file has been added to the content
   * repository.  Implementers of this interface can register to return a
   * modified version of an uploaded file's content.
   *
   * @param poolId
   *          The pool ID of the content object being updated.
   *
   * @param inputStream
   *          The inputStream of the uploaded content.
   *
   * @param contentType
   *          The content type of the uploaded file (as determined by CreateContentPoolServlet)
   *
   * @param value
   *          The RequestParameter object of the file upload request
   *
   **/
  InputStream filterInputStream(String poolId, InputStream inputStream, String contentType, RequestParameter value);
}
