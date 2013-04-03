/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.giraph.hive.tailer;

import com.google.common.base.Joiner;
import com.sampullara.cli.Argument;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.reporting.ConsoleReporter;

import java.util.concurrent.TimeUnit;

public class Opts {
  public static final int METASTORE_PORT = 9083;
  public static final int METRICS_UPDATE_ROWS = 10000;

  @Argument(alias = "h") public boolean help = false;

  @Argument public String clustersFile;
  @Argument public String cluster;

  @Argument public String metastoreHost;
  @Argument public Integer metastorePort = METASTORE_PORT;

  @Argument public String database = "default";
  @Argument(required = true) public String table;
  @Argument(required = true) public String partitionFilter;

  @Argument public Integer threads = 1;
  @Argument public String separator = "\t";

  @Argument public Integer requestNumSplits = 0;
  @Argument public Integer requestSplitsPerThread = 3;

  @Argument public Integer metricsUpdatePeriodRows = METRICS_UPDATE_ROWS;
  @Argument public Integer metricsPrintPeriodSecs = 0;

  public Joiner joiner;

  public static ConsoleReporter metricsReporter() {
    return new ConsoleReporter(Metrics.defaultRegistry(), System.err, MetricPredicate.ALL);
  }

  public void process() {
    joiner = Joiner.on(separator);
    if (metricsPrintPeriodSecs > 0) {
      metricsReporter().start(metricsPrintPeriodSecs, TimeUnit.SECONDS);
    }
  }
}
