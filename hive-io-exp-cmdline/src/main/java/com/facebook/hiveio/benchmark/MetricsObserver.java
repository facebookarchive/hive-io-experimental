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

package com.facebook.hiveio.benchmark;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.hiveio.input.HiveApiInputObserver;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import java.util.concurrent.TimeUnit;

/**
 * Observer that tracks metrics
 */
class MetricsObserver extends HiveApiInputObserver.Empty {
  /** How often to print */
  private final int printPeriod;
  /** Timer to read rows */
  private final Timer readTimer;
  /** Row reading timer context */
  private TimerContext readTimerContext;
  /** Rows read success ratio */
  private final CounterRatioGauge readSuccessRatio;
  /** Parse timer */
  private final Timer parseTimer;
  /** Parse timer context */
  private TimerContext parseTimerContext;

  /**
   * Constructor
   *
   * @param name String name
   * @param printPeriod how often to print
   */
  public MetricsObserver(String name, int printPeriod) {
    TimeUnit durationUnit = TimeUnit.MICROSECONDS;
    TimeUnit rateUnit = TimeUnit.MILLISECONDS;

    this.printPeriod = printPeriod;

    readTimer = Metrics.newTimer(new MetricName(name, "", "reads"),
        durationUnit, rateUnit);
    readSuccessRatio =
        new CounterRatioGauge(Metrics.newCounter(new MetricName(name, "", "successes")),
            Metrics.newCounter(new MetricName(name, "", "-reads")));
    parseTimer = Metrics.newTimer(new MetricName(name, "", "parses"),
        durationUnit, rateUnit);
  }

  public Timer getParseTimer() {
    return parseTimer;
  }

  public CounterRatioGauge getReadSuccessRatio() {
    return readSuccessRatio;
  }

  public Timer getReadTimer() {
    return readTimer;
  }

  @Override
  public void beginReadRow() {
    readTimerContext = readTimer.time();
  }

  @Override
  public void endReadRow(WritableComparable key, Writable value) {
    readSuccessRatio.getNumeratorCounter().inc();
    readSuccessRatio.getDenominatorCounter().inc();
    readTimerContext.stop();
    print(readTimer.count(), printPeriod, "read");
  }

  @Override
  public void hiveReadRowFailed() {
    readSuccessRatio.getDenominatorCounter().inc();
    readTimerContext.stop();
  }

  @Override
  public void beginParse() {
    parseTimerContext = parseTimer.time();
  }

  @Override
  public void endParse(HiveReadableRecord record) {
    parseTimerContext.stop();
  }

  /**
   * Print information
   *
   * @param num Number of rows so far
   * @param printEvery How often to print
   * @param prefix String prefix
   */
  private static void print(long num, int printEvery, String prefix) {
    if (num % printEvery == 0) {
      System.err.println(prefix + " " + num + " records");
    }
  }
}
