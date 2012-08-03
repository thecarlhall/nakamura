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

import org.perf4j.chart.GoogleChartGenerator;
import org.perf4j.chart.StatisticsChartGenerator;
import org.perf4j.log4j.servlet.GraphingServlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 */
public class NakamuraGraphingServlet extends GraphingServlet {
  private static final long serialVersionUID = 1L;

  private final static String PARAM_FOR_GRAPH = "forGraph";

  /**
   * {@inheritDoc}
   * @see org.perf4j.servlet.AbstractGraphingServlet#writeChart(java.lang.String, org.perf4j.chart.StatisticsChartGenerator, javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
   */
  @Override
  protected void writeChart(String name, StatisticsChartGenerator chartGenerator,
      HttpServletRequest request, HttpServletResponse response) throws ServletException,
      IOException {
    GoogleChartGenerator googleChartGenerator = (GoogleChartGenerator) chartGenerator;
    String forGraph = request.getParameter(PARAM_FOR_GRAPH);
    if (forGraph == null) {
      response.getWriter().println("<div>");
      response.getWriter().println("  <h1>"+name+"</h1>");
      response.getWriter().println(String.format("  <iframe src=\"/system/perf4j?%s=%s\" width=\"%s\" height=\"%s\"></iframe>",
          PARAM_FOR_GRAPH, name, String.valueOf(googleChartGenerator.getWidth()), String.valueOf(googleChartGenerator.getHeight())));
      response.getWriter().println("</div>");
    } else if (name.equals(forGraph)) {
      String chartUrl = chartGenerator.getChartUrl();
      
      String[] parts = chartUrl.split("\\?");
      String host = parts[0];
      String queryString = parts[1];
      String[] params = queryString.split("&");
      
      writeHeader(request, response);
      writeChartHeader(host, response);
      
      for (String param : params) {
        parts = param.split("=");
        writeChartParam(parts[0], parts[1], response);
      }
      
      writeChartFooter(response);
      writeFooter(request, response);
      
      response.getWriter().println(chartUrl);
    }
  }

  private void writeChartFooter(HttpServletResponse response) throws IOException {
    response.getWriter().println("</form>");
    response.getWriter().println("<script type=\"text/javascript\">");
    response.getWriter().println("  document.getElementById('chart').submit();");
    response.getWriter().println("</script>");
  }

  private void writeChartParam(String key, String value, HttpServletResponse response) throws IOException {
    response.getWriter().println(String.format(" <input type=\"hidden\" name=\"%s\" value=\"%s\" />",
        key, value));
  }

  private void writeChartHeader(String host, HttpServletResponse response) throws IOException {
    response.getWriter().println("<form id=\"chart\" method=\"POST\" action=\""+host+"\">");
  }
}
