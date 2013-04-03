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

import com.facebook.giraph.hive.common.HiveStats;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

import javax.annotation.concurrent.ThreadSafe;

import static com.barney4j.utils.unit.ByteUnit.BYTE;
import static java.lang.System.err;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
class Stats {
  public long timeNanos;
  public long numRows;
  public Meter rowMeter;
  public Meter rawMBMeter;
  public Meter totalMBMeter;

  public Stats() {
    rowMeter = Metrics.newMeter(Stats.class, "rows", "rows", SECONDS);
    rawMBMeter = Metrics.newMeter(Stats.class, "raw MB (estimated)", "MBs", SECONDS);
    totalMBMeter = Metrics.newMeter(Stats.class, "total MB (estimated)", "MBs", SECONDS);
  }

  public void addRows(HiveStats hiveStats, long numRows) {
    rowMeter.mark(numRows);
    double rowsFraction = numRows / (double) hiveStats.getNumRows();
    rawMBMeter.mark((long) (rowsFraction * hiveStats.getRawSizeInMB()));
    totalMBMeter.mark((long) (rowsFraction * hiveStats.getTotalSizeInMB()));
  }

  public void printEnd(HiveStats hiveStats) {
    Opts.metricsReporter().run();
    final double nsPerRow = timeNanos / (double) numRows;
    final double msecPerRow = NANOSECONDS.toMillis((long) nsPerRow);
    final double rawMBPerNs = hiveStats.getRawSizeInMB() / timeNanos;
    final double rawMBPerSec = SECONDS.toNanos((long) rawMBPerNs);
    final double totalMBPerNs = hiveStats.getTotalSizeInMB() / timeNanos;
    final double totalMBPerSec = BYTE.toMB(SECONDS.toNanos((long) totalMBPerNs));
    err.println("Finished.");
    err.println("  " + numRows + " rows");
    err.println("  " + NANOSECONDS.toSeconds(timeNanos) + " seconds");
    err.println("  " + msecPerRow + " msec / row");
    err.println("  " + rawMBPerSec + " raw MB / second");
    err.println("  " + totalMBPerSec + " total MB / second");
  }
}
