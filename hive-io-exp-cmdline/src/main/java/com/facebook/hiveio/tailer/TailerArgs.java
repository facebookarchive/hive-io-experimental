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

import com.facebook.hiveio.options.Defaults;
import com.facebook.hiveio.options.InputTableOptions;
import com.facebook.hiveio.options.MetastoreOptions;
import com.facebook.hiveio.options.MultiThreadOptions;
import io.airlift.command.Option;

import javax.inject.Inject;

public class TailerArgs {
  @Option(name = "--clustersFile", description = "File of Hive metastore clusters")
  public String clustersFile;

  @Option(name = "--cluster", description = "Cluster to use")
  public String cluster = "silver";

  @Inject public MetastoreOptions metastore = new MetastoreOptions();

  @Inject public InputTableOptions inputTable = new InputTableOptions();

  @Inject public MultiThreadOptions multiThread = new MultiThreadOptions();

  @Option(name = {"--parse-only", "--dont-print"}, description = "Don't print, just measure performance")
  public boolean parseOnly = false;

  @Option(name = {"-l", "--limit"}, description = "Limit on number of rows to process")
  public long limit = Long.MAX_VALUE;

  @Option(name = "--separator", description = "Separator between columns")
  public String separator = Defaults.SEPARATOR;

  @Option(name = "--record-buffer-flush", description = "How many records to buffer before printing")
  public int recordBufferFlush = 1;

  @Option(name = "--request-num_splits", description = "Number of splits to request")
  public int requestNumSplits = 0;

  @Option(name = "--request-splits-per-thread", description = "Number of splits per thread")
  public int requestSplitsPerThread = 3;

  @Option(name = "--append-stats-to",description = "Append final stats to a file")
  public String appendStatsTo;
}
