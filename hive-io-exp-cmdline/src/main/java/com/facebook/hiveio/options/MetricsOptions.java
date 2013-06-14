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

/**
 * Options for metrics
 */
public class MetricsOptions {
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(MetricsOptions.class);

  // CHECKSTYLE: stop VisibilityModifier
  /** toggle system metrics */
  @Option(name = "--metrics-system", description = "Enable system metrics")
  public boolean systemMetrics = false;

  /** network interfaces */
  @Option(name = "--metrics-network-interfaces", description = "Network interfaces to monitor")
  public List<String> networkInterfaces;

  /** stderr print period */
  @Option(name = "--metrics-stderr-seconds", description = "How often to dump metrics to stderr")
  public int stderrPrintSecs = 0;

  /** csv print period */
  @Option(name = "--metrics-csv-seconds", description = "How often to dump metrics to CSV")
  public int csvPrintSecs = 0;

  /** csv output directory */
  @Option(name = "--metrics-csv-directory", description = "Directory to write CSV metrics to")
  public String csvDirectory = "metrics";

  /** every how many rows to update metrics */
  @Option(name = "--metrics-update-period-rows",
      description = "Update metrics every this many records")
  public int updateRows = 1000;
  // CHECKSTYLE: resume VisibilityModifier

  /**
   * Is CSV printing enabled
   *
   * @return true if csv printing enabled
   */
  public boolean csvEnabled() {
    return csvPrintSecs > 0;
  }

  /**
   * Is stderr printing enabled
   *
   * @return true if stderr printing enabled
   */
  public boolean stderrEnabled() {
    return stderrPrintSecs > 0;
  }

  /**
   * Are system metrics enabled
   *
   * @return true if system metrics areenabled
   */
  public boolean systemMetricsEnabled() {
    return systemMetrics;
  }

  /** Dump metrics to stderr */
  public void dumpMetricsToStderr() {
    new ConsoleReporter(System.err).run();
  }

  /** Process options */
  public void process() {
    processSystemMetrics();
    processCsv();
    processStderr();
  }

  /** Process system metrics options */
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

  /** Process csv options */
  private void processCsv() {
    if (csvEnabled()) {
      File dir = new File(csvDirectory);
      try {
        FileUtils.deleteDirectory(dir);
        // CHECKSTYLE: stop EmptyBlock
      } catch (IOException e) { }
      // CHECKSTYLE: resume EmptyBlock
      if (!dir.mkdirs()) {
        LOG.error("Failed to create CSV directory " + dir);
      }
      CsvReporter.enable(dir, csvPrintSecs, TimeUnit.SECONDS);
    }
  }

  /** Process stderr options */
  private void processStderr() {
    if (stderrEnabled()) {
      ConsoleReporter.enable(stderrPrintSecs, TimeUnit.SECONDS);
    }
  }
}
