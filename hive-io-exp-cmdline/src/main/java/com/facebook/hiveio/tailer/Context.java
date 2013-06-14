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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapreduce.InputSplit;

import com.facebook.hiveio.common.HiveStats;
import com.facebook.hiveio.input.HiveApiInputFormat;
import com.facebook.hiveio.schema.HiveTableSchema;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Context for computation
 */
@ThreadSafe
class Context {
  // CHECKSTYLE: stop VisibilityModifier
  /** Hadoop InputFormat */
  public final HiveApiInputFormat hiveApiInputFormat;
  /** HiveConf */
  public final HiveConf hiveConf;
  /** schema */
  public final HiveTableSchema schema;
  /** stats about table */
  public final HiveStats hiveStats;
  /** Queue of input splits to process */
  public Queue<InputSplit> splitsQueue;

  /** stats tracking */
  public final Stats stats;
  /** Number of rows parsed */
  public final AtomicLong rowsParsed;

  /** Per-thread context */
  public final ThreadLocal<ThreadContext> perThread = new ThreadLocal<ThreadContext>() {
    @Override protected ThreadContext initialValue() {
      return new ThreadContext();
    }
  };
  // CHECKSTYLE: resume VisibilityModifier

  /**
   * Constructor
   *
   * @param hiveApiInputFormat Hadoop InputFormat
   * @param hiveConf HiveConf
   * @param schema table schema
   * @param hiveStats table stats
   * @param stats overall stats
   */
  Context(HiveApiInputFormat hiveApiInputFormat, HiveConf hiveConf,
      HiveTableSchema schema, HiveStats hiveStats, Stats stats) {
    this.hiveApiInputFormat = hiveApiInputFormat;
    this.hiveConf = hiveConf;
    this.schema = schema;
    this.hiveStats = hiveStats;
    this.stats = stats;
    this.rowsParsed = new AtomicLong();
  }

  /**
   * Do we have more input splits to read
   *
   * @param limit on how many to read
   * @return true if there are more input splits
   */
  public boolean hasMoreSplitsToRead(long limit) {
    return !splitsQueue.isEmpty() && !limitReached(limit);
  }

  /**
   * Have we reached the limit on input splits
   *
   * @param limit upper bound
   * @return true if limit reached
   */
  public boolean limitReached(long limit) {
    return rowsParsed.get() >= limit;
  }
}
