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


import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.sling.SlingServlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * The <code>OauthServerServlet</code> says Hello World (for the moment)
 */

@SlingServlet(methods={"GET"}, paths={"/system/sling/oauthDemo"})
@Properties(value={
	    @Property(name = "service.description", value = "The Sakai Foundation"),
	    @Property(name = "service.vendor", value = "The Sakai Foundation") 
})

public class OAuthDemoServlet extends HttpServlet {
//public class OauthServerServlet extends SlingAllMethodsServlet {
	
	//private OauthServerHandler oauthHandler;
	/**
	 *
	 */
	private static final long serialVersionUID = -2002186252317448037L;

	/**
	 * {@inheritDoc}
	 * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	*/ 
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		OAuthParams oauthParams = null;
        oauthParams.setAuthzEndpoint(Utils.FACEBOOK_AUTHZ);
        oauthParams.setTokenEndpoint(Utils.FACEBOOK_TOKEN);

		//oauthHandler = new OauthServerHandler();
        /*Session session = StorageClientUtils.adaptToSession(request.getResourceResolver().adaptTo(javax.jcr.Session.class));
        ContentManager cm = session.getContentManager();
        Content content = new Content(LitePersonalUtils.getPrivatePath(userId) + "/oauth", ImmutableMap.of("authorization_token", authorizationToken));
        cm.update(content);*/

		resp.getWriter().write("Hello World from Oauth Server: " );

	}


}