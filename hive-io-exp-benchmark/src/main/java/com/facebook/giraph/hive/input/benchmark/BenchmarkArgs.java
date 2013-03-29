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

package com.facebook.giraph.hive.input.benchmark;

import com.sampullara.cli.Argument;

/**
 *
 */
class BenchmarkArgs {
  /** Hive host */
  @Argument private String hiveHost = "hadoopminimstr032.frc1.facebook.com";

  /** Hive port */
  @Argument private int hivePort = 9083;

  /** Hive database */
  @Argument private String database = "default";

  /** Hive table */
  @Argument private String table = "inference_sims";

  /** Partition filter */
  @Argument private String partitionFilter = "ds='2013-01-01' and feature='test_nz'";

  /** Whether to track metrics */
  @Argument private boolean trackMetrics = false;

  /** Every how many splits to print */
  @Argument private int splitPrintPeriod = 3;

  /** Every how many records in a split to print */
  @Argument private int recordPrintPeriod = 1000000;

  /** Print usage */
  @Argument private boolean help = false;

  public boolean isTrackMetrics() {
    return trackMetrics;
  }

  public int getSplitPrintPeriod() {
    return splitPrintPeriod;
  }

  public int getRecordPrintPeriod() {
    return recordPrintPeriod;
  }

  public boolean isHelp() {
    return help;
  }

  public String getHiveHost() {
    return hiveHost;
  }

  public int getHivePort() {
    return hivePort;
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  public String getPartitionFilter() {
    return partitionFilter;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public void setHelp(boolean help) {
    this.help = help;
  }

  public void setHiveHost(String hiveHost) {
    this.hiveHost = hiveHost;
  }

  public void setHivePort(int hivePort) {
    this.hivePort = hivePort;
  }

  public void setPartitionFilter(String partitionFilter) {
    this.partitionFilter = partitionFilter;
  }

  public void setTable(String table) {
    this.table = table;
  }
}
