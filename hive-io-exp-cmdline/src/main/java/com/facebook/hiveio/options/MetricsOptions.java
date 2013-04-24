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
package com.facebook.hiveio.options;

import org.apache.commons.io.FileUtils;
import org.hyperic.sigar.SigarException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.SigarNative;
import com.facebook.hiveio.metrics.NetworkMetrics;
import com.facebook.hiveio.metrics.SigarMetrics;
import com.yammer.metrics.reporting.ConsoleReporter;
import com.yammer.metrics.reporting.CsvReporter;
import io.airlift.command.Option;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MetricsOptions {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsOptions.class);

  @Option(name = "--metrics-system", description = "Enable system metrics")
  public boolean systemMetrics = false;

  @Option(name = "--metrics-network-interfaces", description = "Network interfaces to monitor")
  public List<String> networkInterfaces;

  @Option(name = "--metrics-stderr-seconds", description = "How often to dump metrics to stderr")
  public int stderrPrintSecs = 0;

  @Option(name = "--metrics-csv-seconds", description = "How often to dump metrics to CSV")
  public int csvPrintSecs = 0;

  @Option(name = "--metrics-csv-directory", description = "Directory to write CSV metrics to")
  public String csvDirectory = "metrics";

  @Option(name = "--metrics-update-period-rows", description = "Update metrics every this many records")
  public int updateRows = 1000;

  public boolean csvEnabled() {
    return csvPrintSecs > 0;
  }

  public boolean stderrEnabled() {
    return stderrPrintSecs > 0;
  }

  public boolean systemMetricsEnabled() {
    return systemMetrics;
  }

  public void dumpMetricsToStderr() {
    new ConsoleReporter(System.err).run();
  }

  public void process() {
    processSystemMetrics();
    processCsv();
    processStderr();
  }

  private void processSystemMetrics() {
    if (systemMetricsEnabled()) {
      SigarNative.requireSigarNative();
      SigarMetrics.registerMetrics();
      try {
        NetworkMetrics.registerMetrics(networkInterfaces);
      } catch (SigarException e) {
        LOG.error("Could not init network metrics", e);
      }
    }
  }

  private void processCsv() {
    if (csvEnabled()) {
      File dir = new File(csvDirectory);
      try {
        FileUtils.deleteDirectory(dir);
      } catch (IOException e) {}
      dir.mkdirs();
      CsvReporter.enable(dir, csvPrintSecs, TimeUnit.SECONDS);
    }
  }

  private void processStderr() {
    if (stderrEnabled()) {
      ConsoleReporter.enable(stderrPrintSecs, TimeUnit.SECONDS);
    }
  }
}
