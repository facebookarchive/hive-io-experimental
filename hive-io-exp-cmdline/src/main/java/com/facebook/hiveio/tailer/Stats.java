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
package com.facebook.hiveio.tailer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.HiveStats;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.reporting.ConsoleReporter;

import javax.annotation.concurrent.ThreadSafe;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicLong;

import static com.barney4j.utils.unit.ByteUnit.BYTE;
import static com.barney4j.utils.unit.ByteUnit.MB;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Statistics tracked for hivetail
 */
@ThreadSafe
class Stats {
  /** flush period */
  public static final long MEGABYTES_FLUSH = 10;

  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(Stats.class);

  /** hive stats */
  private final HiveStats hiveStats;

  /** rows */
  private Meter rowMeter;

  /** MBs */
  private Meter rawMBMeter;
  /** bytes left till next flush */
  private AtomicLong bytesTillFlush = new AtomicLong();

  /**
   * Constructor
   *
   * @param hiveStats hive stats
   */
  private Stats(HiveStats hiveStats) {
    this.hiveStats = hiveStats;
    rowMeter = Metrics.newMeter(Stats.class, "rows", "rows", SECONDS);
    rawMBMeter = Metrics.newMeter(Stats.class, "megabytes (estimated)", "MBs", SECONDS);
  }

  /**
   * Create and get instance
   *
   * @param hiveStats hive stats
   * @return Stats
   */
  public static Stats create(HiveStats hiveStats) {
    return new Stats(hiveStats);
  }

  /**
   * Add rows to stats
   *
   * @param numRows number of rows
   */
  public void addRows(long numRows) {
    rowMeter.mark(numRows);
    double rowsFraction = numRows / (double) hiveStats.getNumRows();
    double bytesFraction = rowsFraction * hiveStats.getRawSizeInBytes();
    long bytes = (long) bytesFraction;
    addBytes(bytes);
  }

  /**
   * Add bytes to stats
   *
   * @param bytes number of bytes
   */
  private void addBytes(long bytes) {
    long bytesSoFar = bytesTillFlush.addAndGet(bytes);
    while (bytesSoFar > MB.toBytes(MEGABYTES_FLUSH)) {
      if (bytesTillFlush.compareAndSet(bytesSoFar, 0)) {
        rawMBMeter.mark((long) BYTE.toMB(bytesSoFar));
      }
      bytesSoFar = bytesTillFlush.get();
    }
  }

  /**
   * Get metrics reporter
   *
   * @return ConsoleReporter
   */
  public static ConsoleReporter metricsReporter() {
    return new ConsoleReporter(Metrics.defaultRegistry(), System.err, MetricPredicate.ALL);
  }

  /**
   * Print end results
   *
   * @param context Context
   * @param args TailerArgs
   * @param timeNanos nanoseconds
   * @param stream OutputStream
   * @throws IOException
   */
  public void printEndBenchmark(Context context, TailerArgs args, long timeNanos,
      OutputStream stream) throws IOException {
    PrintWriter writer = new PrintWriter(new BufferedWriter(
        new OutputStreamWriter(stream, Charsets.UTF_8)));
    Joiner joiner = Joiner.on(",");

    long seconds = NANOSECONDS.toSeconds(timeNanos);
    double rowsPerSec = rowMeter.count() / (double) seconds;
    double mbPerSec = mbParsed(hiveStats) / (double) seconds;

    writer.println(joiner.join(
        rowMeter.count(),
        args.multiThread.threads,
        rawMBMeter.count(),
        seconds,
        rowsPerSec,
        mbPerSec));
    writer.flush();
  }

  /**
   * Compute how many MBs we've parsed
   *
   * @param hiveStats HiveStats
   * @return megabytes parsed
   */
  private double mbParsed(HiveStats hiveStats) {
    double rowsPct = rowMeter.count() / (double) hiveStats.getNumRows();
    return hiveStats.getRawSizeInMB() * rowsPct;
  }
}
