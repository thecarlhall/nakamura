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

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.amber.oauth2.client.OAuthClient;
import org.apache.amber.oauth2.client.URLConnectionClient;
import org.apache.amber.oauth2.client.request.OAuthClientRequest;
import org.apache.amber.oauth2.client.response.GitHubTokenResponse;
import org.apache.amber.oauth2.client.response.OAuthAccessTokenResponse;
import org.apache.amber.oauth2.client.response.OAuthAuthzResponse;
import org.apache.amber.oauth2.common.exception.OAuthProblemException;
import org.apache.amber.oauth2.common.exception.OAuthSystemException;
import org.apache.amber.oauth2.common.message.types.GrantType;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.util.HttpURLConnection;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.sling.SlingServlet;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.servlets.SlingAllMethodsServlet;
import org.apache.sling.commons.osgi.PropertiesUtil;

@SlingServlet(methods = { "GET","POST" }, paths = { "/system/sling/oauthDemoVerifier" })
@Properties(value = {
    @Property(name = "service.description", value = "The Sakai Foundation"),
    @Property(name = "service.vendor", value = "The Sakai Foundation") })
public class OAuthDemoVerifyServlet extends SlingAllMethodsServlet {
  /**
	 * 
	 */
  private static final long serialVersionUID = 1L;

  // TODO require this be provided in the configuration
  public static final String DEFAULT_TOKEN_LOCATION = "https://accounts.google.com/o/oauth2/token";
  @Property(value = OAuthDemoVerifyServlet.DEFAULT_TOKEN_LOCATION)
  static final String TOKEN_LOCATION = "tokenLocation";

  // TODO require this be provided in the configuration
  public static final String DEFAULT_CLIENT_ID = "215879716306.apps.googleusercontent.com";
  @Property(value = OAuthDemoVerifyServlet.DEFAULT_CLIENT_ID)
  static final String CLIENT_ID = "clientId";
  
  // TODO require this be provided in the configuration
  public static final String DEFAULT_CLIENT_SECRET = "NIsboWRtRfthhZMobVLGeis0";
  @Property(value = OAuthDemoVerifyServlet.DEFAULT_CLIENT_SECRET)
  static final String CLIENT_SECRET = "clientSecret";
  
  public static final String DEFAULT_REDIRECT_URI = "http://localhost:8080/system/sling/oauthDemoVerifier";
  @Property(value = OAuthDemoVerifyServlet.DEFAULT_REDIRECT_URI)
  static final String REDIRECT_URI = "redirectUri";
  
  private String tokenLocation;
  private String clientId;
  private String clientSecret;
  private String redirectUri;
  
  @Activate
  protected void activate(Map<?, ?> props) {
    tokenLocation = PropertiesUtil.toString(props.get(TOKEN_LOCATION), DEFAULT_TOKEN_LOCATION);
    clientId = PropertiesUtil.toString(props.get(CLIENT_ID), DEFAULT_CLIENT_ID);
    clientSecret = PropertiesUtil.toString(props.get(CLIENT_SECRET), DEFAULT_CLIENT_SECRET);
    redirectUri = PropertiesUtil.toString(props.get(REDIRECT_URI), DEFAULT_REDIRECT_URI);
  }
  
  private OAuthClientRequest getToken(String code){  
    OAuthClientRequest  oar_request = null;
   try {
      oar_request = OAuthClientRequest
         .tokenLocation(tokenLocation)
         .setCode(code)
         .setClientId(clientId)
         .setClientSecret(clientSecret)
         .setRedirectURI(redirectUri)
         .setGrantType(GrantType.AUTHORIZATION_CODE)
         .buildBodyMessage();
     
   } catch (OAuthSystemException e) {
     // TODO Auto-generated catch block
     e.printStackTrace();
   }
    return oar_request;
    
  } 
  
  private void exchangeTokens(String code){
    String urlParameters = "code="+ code + 
        "&client_id="+ clientId +
        "&client_secret=" + clientSecret +
        "&redirect_uri=" + redirectUri +
        "&grant_type=authorization_code";
    URL url;
    try {
      url = new URL(tokenLocation);
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();           
      connection.setDoOutput(true);
      connection.setDoInput(true);
      connection.setInstanceFollowRedirects(false); 
      connection.setRequestMethod("POST"); 
      connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded"); 
      connection.setRequestProperty("charset", "utf-8");
      connection.setRequestProperty("Content-Length", "" + Integer.toString(urlParameters.getBytes().length));
      connection.setUseCaches (false);

      DataOutputStream wr = new DataOutputStream(connection.getOutputStream ());
      wr.writeBytes(urlParameters);
      wr.flush();
      wr.close();
      connection.disconnect();

    } catch (MalformedURLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } 
     }

  protected void doGet(SlingHttpServletRequest request, SlingHttpServletResponse response)
      throws ServletException, IOException {
    
    OAuthAuthzResponse oar = null;
    try {
      oar = OAuthAuthzResponse.oauthCodeAuthzResponse(request);      
    } catch (OAuthProblemException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    String code = oar.getCode();
    

    response.getWriter().write( "Hello World from Oauth Server: " + code );

    try {
      OAuthClientRequest oauthRequest = OAuthClientRequest
          .tokenLocation(tokenLocation)
          .setCode(code)
          .setClientId(clientId)
          .setClientSecret(clientSecret)
          .setRedirectURI(redirectUri)
          .setGrantType(GrantType.AUTHORIZATION_CODE)
          .buildBodyMessage();
      
      OAuthClient client = new OAuthClient(new URLConnectionClient());
      OAuthAccessTokenResponse oauthResponse = null;
      oauthResponse = client.accessToken(oauthRequest);

      //response.getWriter().append("Access Token: " + oauthResponse.getAccessToken());
      
      response.sendRedirect(oauthRequest.getLocationUri());

    } catch (OAuthSystemException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (OAuthProblemException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    
   //response.getWriter().append( "Hello World from Oauth Server: " + code );

  }
  
  protected void doPost(SlingHttpServletRequest request, SlingHttpServletResponse response)
      throws ServletException, IOException {
    response.getWriter().append( "Hello World from Oauth ");
    OAuthAuthzResponse oar = null;
    try {
      oar = OAuthAuthzResponse.oauthCodeAuthzResponse(request);      
    } catch (OAuthProblemException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    String code = oar.getCode();
    response.getWriter().write( "Hello World from Oauth Server: " + code );

  }
}
