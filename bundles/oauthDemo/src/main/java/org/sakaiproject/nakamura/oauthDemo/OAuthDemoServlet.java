/*
 * Licensed to the Sakai Foundation (SF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The SF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */


package org.sakaiproject.nakamura.oauthDemo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.amber.oauth2.client.request.OAuthClientRequest;
import org.apache.amber.oauth2.common.exception.OAuthSystemException;
import org.apache.felix.scr.annotations.sling.SlingServlet;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.servlets.SlingAllMethodsServlet;

import java.io.IOException;
import javax.servlet.ServletException;

/**
 * The <code>OauthServerServlet</code> says Hello World (for the moment)
 */

@SlingServlet(methods={"GET","POST"}, paths={"/system/sling/oauthDemo"})
public class OAuthDemoServlet extends SlingAllMethodsServlet {
	 
	private static final Logger LOGGER = LoggerFactory.getLogger(OAuthDemoServlet.class);

	/**
	 *
	 */
	private static final long serialVersionUID = -2002186252317448037L;

	/**
	 * {@inheritDoc}
	 * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	*/ 
	protected void doGet(SlingHttpServletRequest request, SlingHttpServletResponse response)
			throws ServletException, IOException {
	  dispatch(request, response);
	}

  protected void doPost(SlingHttpServletRequest request, SlingHttpServletResponse response)
			throws ServletException, IOException {
		dispatch(request, response);
	}

  private void dispatch(SlingHttpServletRequest request, SlingHttpServletResponse response)
      throws ServletException, IOException {
    try {
      // TODO make all of these settings configurable via OSGi properties
      OAuthClientRequest oauthRequest = OAuthClientRequest
          .authorizationLocation("https://accounts.google.com/o/oauth2/auth")
          .setScope("http://www.googleapis.com/auth/userinfo.email https://www.googleapis.com/auth/userinfo.profile")
          .setState("/profile")
          .setRedirectURI("http://localhost:8080/system/sling/oauthDemoVerifier")
          .setResponseType("code")
          .setClientId("812741506391.apps.googleusercontent.com")
          .setParameter("approval_prompt", "force")
          .buildQueryMessage();
      response.sendRedirect(oauthRequest.getLocationUri());
    } catch (OAuthSystemException e) {
      throw new ServletException(e.getMessage(), e);
    }
  }
}