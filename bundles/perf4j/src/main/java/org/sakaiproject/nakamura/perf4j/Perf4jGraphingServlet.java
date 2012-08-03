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
package org.sakaiproject.nakamura.perf4j;

import org.apache.felix.scr.annotations.sling.SlingServlet;
import org.apache.sling.api.servlets.SlingSafeMethodsServlet;

import java.io.IOException;
import java.util.Enumeration;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

/**
 * This Servlet acts as a concrete component stub, simply to provide the
 * {@code GraphingServlet} as an OSGi component.
 */
@SlingServlet(paths = "/system/perf4j", generateComponent = true, generateService = true, methods = { "GET" })
public class Perf4jGraphingServlet extends SlingSafeMethodsServlet {
  private static final long serialVersionUID = 1L;

  /**
   * Delegate Graphing servlet that this Sling servlet wraps.
   */
  private static NakamuraGraphingServlet servlet = new NakamuraGraphingServlet();

  /**
   * @throws ServletException
   * @see org.perf4j.servlet.AbstractGraphingServlet#init()
   */
  public void init() throws ServletException {
    servlet.init();
  }

  /**
   * 
   * @see org.perf4j.servlet.AbstractGraphingServlet#destroy()
   */
  public void destroy() {
    servlet.destroy();
  }

  /**
   * @param name
   * @return
   * @see javax.servlet.GenericServlet#getInitParameter(java.lang.String)
   */
  public String getInitParameter(String name) {
    return servlet.getInitParameter(name);
  }

  /**
   * @return
   * @see javax.servlet.GenericServlet#getInitParameterNames()
   */
  @SuppressWarnings("rawtypes")
  public Enumeration getInitParameterNames() {
    return servlet.getInitParameterNames();
  }

  /**
   * @return
   * @see javax.servlet.GenericServlet#getServletConfig()
   */
  public ServletConfig getServletConfig() {
    return servlet.getServletConfig();
  }

  /**
   * @return
   * @see javax.servlet.GenericServlet#getServletContext()
   */
  public ServletContext getServletContext() {
    return servlet.getServletContext();
  }

  /**
   * @return
   * @see javax.servlet.GenericServlet#getServletInfo()
   */
  public String getServletInfo() {
    return servlet.getServletInfo();
  }

  /**
   * @param config
   * @throws ServletException
   * @see javax.servlet.GenericServlet#init(javax.servlet.ServletConfig)
   */
  public void init(ServletConfig config) throws ServletException {
    servlet.init(config);
  }

  /**
   * @param msg
   * @see javax.servlet.GenericServlet#log(java.lang.String)
   */
  public void log(String msg) {
    servlet.log(msg);
  }

  /**
   * @param message
   * @param t
   * @see javax.servlet.GenericServlet#log(java.lang.String, java.lang.Throwable)
   */
  public void log(String message, Throwable t) {
    servlet.log(message, t);
  }

  /**
   * @return
   * @see javax.servlet.GenericServlet#getServletName()
   */
  public String getServletName() {
    return servlet.getServletName();
  }

  /**
   * @param req
   * @param res
   * @throws ServletException
   * @throws IOException
   * @see javax.servlet.http.HttpServlet#service(javax.servlet.ServletRequest, javax.servlet.ServletResponse)
   */
  public void service(ServletRequest req, ServletResponse res) throws ServletException,
      IOException {
    servlet.service(req, res);
  }
}
