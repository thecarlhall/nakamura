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

import org.perf4j.GroupedTimingStatistics;
import org.perf4j.TimingStatistics;
import org.perf4j.chart.GoogleChartGenerator;
import org.perf4j.helpers.StatsValueRetriever;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 */
public class NakamuraGoogleChartsGenerator extends GoogleChartGenerator {

  protected StatsValueRetriever valueRetriever;
  
  /**
   * 
   */
  public NakamuraGoogleChartsGenerator() {
    super();
  }

  /**
   * @param valueRetriever
   * @param baseUrl
   */
  public NakamuraGoogleChartsGenerator(StatsValueRetriever valueRetriever, String baseUrl) {
    super(valueRetriever, baseUrl);
    this.valueRetriever = valueRetriever;
  }

  /**
   * @param statsValueRetriever
   */
  public NakamuraGoogleChartsGenerator(StatsValueRetriever statsValueRetriever) {
    super(statsValueRetriever);
    this.valueRetriever = statsValueRetriever;
  }

  /**
   * This has been copy / pasted from {@link org.perf4j.chart.GoogleChartGenerator#generateGoogleChartParams()}
   * for the purpose of overriding just the "enabledTag" functionality. Since many (all?) tags will be dynamic,
   * we are going to enhance the functionality by doing a prefix check, instead of exact equality check.
   * <p>
   * The benefit is that you can, for example, graph all "HTTP module" tags by enabling: "http:" in the logging
   * configuration.
   * 
   * @see org.perf4j.chart.GoogleChartGenerator#generateGoogleChartParams()
   */
  @SuppressWarnings("unchecked")
  @Override
  protected String generateGoogleChartParams() {
      long minTimeValue = Long.MAX_VALUE;
      long maxTimeValue = Long.MIN_VALUE;
      double maxDataValue = Double.MIN_VALUE;
      //this map stores all the data series. The key is the tag name (each tag represents a single series) and the
      //value contains two lists of numbers - the first list contains the X values for each point (which is time in
      //milliseconds) and the second list contains the y values, which are the data values pulled from dataWindows.
      Map<String, List<Number>[]> tagsToXDataAndYData = new TreeMap<String, List<Number>[]>();

      for (GroupedTimingStatistics groupedTimingStatistics : getData()) {
          Map<String, TimingStatistics> statsByTag = groupedTimingStatistics.getStatisticsByTag();
          long windowStartTime = groupedTimingStatistics.getStartTime();
          long windowLength = groupedTimingStatistics.getStopTime() - windowStartTime;
          //keep track of the min/max time value, this is needed for scaling the chart parameters
          minTimeValue = Math.min(minTimeValue, windowStartTime);
          maxTimeValue = Math.max(maxTimeValue, windowStartTime);

          for (Map.Entry<String, TimingStatistics> tagWithData : statsByTag.entrySet()) {
              String tag = tagWithData.getKey();
              if (isTagEnabled(tag)) {
                  //get the corresponding value from tagsToXDataAndYData
                  List<Number>[] xAndYData = tagsToXDataAndYData.get(tagWithData.getKey());
                  if (xAndYData == null) {
                      tagsToXDataAndYData.put(tag, xAndYData = new List[]{new ArrayList<Number>(),
                                                                          new ArrayList<Number>()});
                  }

                  //the x data is the start time of the window, the y data is the value
                  Number yValue = this.valueRetriever.getStatsValue(tagWithData.getValue(), windowLength);
                  xAndYData[0].add(windowStartTime);
                  xAndYData[1].add(yValue);

                  //update the max data value, which is needed for scaling
                  maxDataValue = Math.max(maxDataValue, yValue.doubleValue());
              }
          }
      }

      //if it's empty, there's nothing to display
      if (tagsToXDataAndYData.isEmpty()) {
          return "";
      }

      //set up the axis labels - we use the US decimal format locale to ensure the decimal separator is . and not ,
      DecimalFormat decimalFormat = new DecimalFormat("##0.0", new DecimalFormatSymbols(Locale.US));
      SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
      dateFormat.setTimeZone(GroupedTimingStatistics.getTimeZone());

      //the y-axis label goes from 0 to the maximum data value
      String axisRangeParam = "&chxr=2,0," + decimalFormat.format(maxDataValue);

      //for the x-axis (time) labels, ideally we want one label for each data window, but support a maximum of 10
      //labels so the chart doesn't get too crowded
      int stepSize = getData().size() / 10 + 1;
      StringBuilder timeAxisLabels = new StringBuilder("&chxl=0:");
      StringBuilder timeAxisLabelPositions = new StringBuilder("&chxp=0");

      for (Iterator<GroupedTimingStatistics> iter = getData().iterator(); iter.hasNext();) {
          GroupedTimingStatistics groupedTimingStatistics = iter.next();
          long windowStartTime = groupedTimingStatistics.getStartTime();
          String label = dateFormat.format(new Date(windowStartTime));
          double position = 100.0 * (windowStartTime - minTimeValue) / (maxTimeValue - minTimeValue);
          timeAxisLabels.append("|").append(label);
          timeAxisLabelPositions.append(",").append(decimalFormat.format(position));

          //skip over some windows if stepSize is greater than 1
          for (int i = 1; i < stepSize && iter.hasNext(); i++) {
              iter.next();
          }
      }

      //this next line appends a "Time" label in the middle of the bottom of the X axis
      timeAxisLabels.append("|1:|Time");
      timeAxisLabelPositions.append("|1,50");

      //display the gridlines
      double xAxisGridlineStepSize = getData().size() > 2 ? 100.0 / (getData().size() - 1) : 50.0;
      String gridlinesParam = "&chg=" + decimalFormat.format(xAxisGridlineStepSize) + ",10";

      //at this point we should be able to normalize the data to 0 - 100 as required by the google chart API
      StringBuilder chartDataParam = new StringBuilder("&chd=t:");
      StringBuilder chartColorsParam = new StringBuilder("&chco=");
      StringBuilder chartShapeMarkerParam = new StringBuilder("&chm=");
      StringBuilder chartLegendParam = new StringBuilder("&chdl=");

      //this loop is run once for each tag, i.e. each data series to be displayed on the chart
      int i = 0;
      for (Iterator<Map.Entry<String, List<Number>[]>> iter = tagsToXDataAndYData.entrySet().iterator();
           iter.hasNext(); i++) {
          Map.Entry<String, List<Number>[]> tagWithXAndYData = iter.next();

          //data param
          List<Number> xValues = tagWithXAndYData.getValue()[0];
          chartDataParam.append(numberValuesToGoogleDataSeriesParam(xValues, minTimeValue, maxTimeValue));
          chartDataParam.append("|");

          List<Number> yValues = tagWithXAndYData.getValue()[1];
          chartDataParam.append(numberValuesToGoogleDataSeriesParam(yValues, 0, maxDataValue));

          //color param
          String color = DEFAULT_SERIES_COLORS[i % DEFAULT_SERIES_COLORS.length];
          chartColorsParam.append(color);

          //the shape marker param puts a diamond (the d) at each data point (the -1) of size 5 pixels.
          chartShapeMarkerParam.append("d,").append(color).append(",").append(i).append(",-1,5.0");

          //legend param
          chartLegendParam.append(tagWithXAndYData.getKey());

          if (iter.hasNext()) {
              chartDataParam.append("|");
              chartColorsParam.append(",");
              chartShapeMarkerParam.append("|");
              chartLegendParam.append("|");
          }
      }

      return chartDataParam.toString()
             + chartColorsParam
             + chartShapeMarkerParam
             + chartLegendParam
             + axisRangeParam
             + timeAxisLabels
             + timeAxisLabelPositions
             + gridlinesParam;
  }

  /**
   * Determine whether or not the given profiling tag is enabled for this chart.
   * 
   * @param tag
   * @return
   */
  protected boolean isTagEnabled(String tag) {
    if (getEnabledTags() == null)
      return true;
    for (String enabledTag : getEnabledTags()) {
      if (tag.startsWith(enabledTag))
        return true;
    }
    return false;
  }


}
