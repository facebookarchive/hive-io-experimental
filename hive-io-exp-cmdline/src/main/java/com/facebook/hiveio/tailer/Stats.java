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

import com.barney4j.utils.unit.ByteUnit;
import com.facebook.hiveio.common.HiveStats;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
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
import static java.lang.System.err;
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
    rawMBMeter = Metrics.newMeter(Stats.class, "raw MB (estimated)", "MBs", SECONDS);
    Metrics.newGauge(Stats.class, "used MB", new Gauge<Long>() {
      @Override public Long value() {
        Runtime runtime = Runtime.getRuntime();
        return (long) ByteUnit.BYTE.toMB(runtime.totalMemory() - runtime.freeMemory());
      }
    });
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

  public void printEnd(long timeNanos) throws IOException {
    metricsReporter().run();

    final double nsPerRow = Math.floor(timeNanos / (double) rowMeter.count());
    final double msecPerRow = NANOSECONDS.toMillis((long) nsPerRow);
    final double rawMBPerNs = hiveStats.getRawSizeInMB() / timeNanos;
    final double rawMBPerSec = SECONDS.toNanos((long) rawMBPerNs);
    final double totalMBPerNs = hiveStats.getTotalSizeInMB() / timeNanos;
    final double totalMBPerSec = BYTE.toMB(SECONDS.toNanos((long) totalMBPerNs));

    err.println("Finished.");
    err.println("  " + rowMeter.count() + " rows");
    err.println("  " + NANOSECONDS.toSeconds(timeNanos) + " seconds");
    err.println("  " + nsPerRow + " ns / row");
    err.println("  " + msecPerRow + " msec / row");
    err.println("  " + rawMBPerNs + " raw MB / ns");
    err.println("  " + rawMBPerSec + " raw MB / second");
    err.println("  " + totalMBPerNs + " total MB / ns");
    err.println("  " + totalMBPerSec + " total MB / second");
  }

  public void printEndBenchmark(long timeNanos, OutputStream stream) throws IOException {
    final PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(stream)));

    final double nsPerRow = Math.floor(timeNanos / (double) rowMeter.count());
    final double msecPerRow = NANOSECONDS.toMillis((long) nsPerRow);
    final double rawMBPerNs = hiveStats.getRawSizeInMB() / timeNanos;
    final double rawMBPerSec = SECONDS.toNanos((long) rawMBPerNs);
    final double totalMBPerNs = hiveStats.getTotalSizeInMB() / timeNanos;
    final double totalMBPerSec = BYTE.toMB(SECONDS.toNanos((long) totalMBPerNs));


    writer.println("Finished.");
    writer.println("  " + rowMeter.count() + " rows");
    writer.println("  " + NANOSECONDS.toSeconds(timeNanos) + " seconds");
    writer.println("  " + nsPerRow + " ns / row");
    writer.println("  " + msecPerRow + " msec / row");
    writer.println("  " + rawMBPerNs + " raw MB / ns");
    writer.println("  " + rawMBPerSec + " raw MB / second");
    writer.println("  " + totalMBPerNs + " total MB / ns");
    writer.println("  " + totalMBPerSec + " total MB / second");
  }
}
