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

@ThreadSafe
class Stats {
  public static final long MEGABYTES_FLUSH = 10;

  private static final Logger LOG = LoggerFactory.getLogger(Stats.class);

  private final HiveStats hiveStats;

  public Meter rowMeter;

  public Meter rawMBMeter;
  public AtomicLong bytesTillFlush = new AtomicLong();

  public static Stats instance;

  private Stats(HiveStats hiveStats) {
    this.hiveStats = hiveStats;
    rowMeter = Metrics.newMeter(Stats.class, "rows", "rows", SECONDS);
    rawMBMeter = Metrics.newMeter(Stats.class, "megabytes (estimated)", "MBs", SECONDS);
  }

  public static Stats get(HiveStats hiveStats) {
    instance = new Stats(hiveStats);
    return instance;
  }

  public void addRows(long numRows) {
    rowMeter.mark(numRows);
    double rowsFraction = numRows / (double) hiveStats.getNumRows();
    double bytesFraction = rowsFraction * hiveStats.getRawSizeInBytes();
    long bytes = (long) bytesFraction;
    addBytes(bytes);
  }

  private void addBytes(long bytes) {
    long bytesSoFar = bytesTillFlush.addAndGet(bytes);
    while (bytesSoFar > MB.toBytes(MEGABYTES_FLUSH)) {
      if (bytesTillFlush.compareAndSet(bytesSoFar, 0)) {
        rawMBMeter.mark((long) BYTE.toMB(bytesSoFar));
      }
      bytesSoFar = bytesTillFlush.get();
    }
  }

  public static ConsoleReporter metricsReporter() {
    return new ConsoleReporter(Metrics.defaultRegistry(), System.err, MetricPredicate.ALL);
  }

  public void printEndBenchmark(Context context, long timeNanos,
      OutputStream stream) throws IOException {
    PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(stream)));
    Joiner joiner = Joiner.on(",");

    long seconds = NANOSECONDS.toSeconds(timeNanos);
    double rowsPerSec = rowMeter.count() / (double) seconds;
    double mbPerSec = mbParsed(hiveStats) / (double) seconds;

    writer.println(joiner.join(
        rowMeter.count(),
        context.opts.threads,
        rawMBMeter.count(),
        seconds,
        rowsPerSec,
        mbPerSec));
    writer.flush();
  }

  private double mbParsed(HiveStats hiveStats) {
    double rowsPct = rowMeter.count() / (double) hiveStats.getNumRows();
    return hiveStats.getRawSizeInMB() * rowsPct;
  }
}
