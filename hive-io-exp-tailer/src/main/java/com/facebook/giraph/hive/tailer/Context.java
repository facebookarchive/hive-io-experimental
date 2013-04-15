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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapreduce.InputSplit;

import com.facebook.giraph.hive.common.HiveStats;
import com.facebook.giraph.hive.input.HiveApiInputFormat;
import com.facebook.giraph.hive.schema.HiveTableSchema;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
class Context {
  public final HiveApiInputFormat hiveApiInputFormat;
  public final HiveConf hiveConf;
  public final HiveTableSchema schema;
  public final HiveStats hiveStats;
  public Queue<InputSplit> splitsQueue;

  public final Opts opts;
  public final Stats stats;
  public final AtomicLong rowsParsed;

  public final ThreadLocal<ThreadContext> threadContext = new ThreadLocal<ThreadContext>() {
    @Override protected ThreadContext initialValue() {
      return new ThreadContext();
    }
  };

  Context(HiveApiInputFormat hiveApiInputFormat, HiveConf hiveConf,
      HiveTableSchema schema, HiveStats hiveStats, Opts opts, Stats stats) {
    this.hiveApiInputFormat = hiveApiInputFormat;
    this.hiveConf = hiveConf;
    this.schema = schema;
    this.hiveStats = hiveStats;
    this.opts = opts;
    this.stats = stats;
    this.rowsParsed = new AtomicLong();
  }

  public boolean hasMoreSplitsToRead() {
    return !splitsQueue.isEmpty() && !limitReached();
  }

  public boolean limitReached() {
    return rowsParsed.get() >= opts.limit;
  }
}
