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
import org.perf4j.helpers.StatsValueRetriever;
import org.perf4j.log4j.GraphingStatisticsAppender;

/**
 * Graphing statistics appender that will return the Nakamura custom GoogleChartGenerator as its
 * Graph Generator.
 */
public class NakamuraGraphingStatisticsAppender extends GraphingStatisticsAppender {

  /**
   * {@inheritDoc}
   * @see org.perf4j.log4j.GraphingStatisticsAppender#createChartGenerator()
   */
  @Override
  protected StatisticsChartGenerator createChartGenerator() {
    GoogleChartGenerator parent = (GoogleChartGenerator) super.createChartGenerator();
    StatsValueRetriever statsValueRetriever = StatsValueRetriever.DEFAULT_RETRIEVERS.get(getGraphType());
    NakamuraGoogleChartsGenerator result = new NakamuraGoogleChartsGenerator(statsValueRetriever);
    result.setEnabledTags(parent.getEnabledTags());
    return result;
  }

}
