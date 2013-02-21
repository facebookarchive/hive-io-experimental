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

package com.facebook.giraph.hive.impl.input.benchmark;

import com.sampullara.cli.Argument;

/**
 *
 */
public class BenchmarkArgs {
  /** Hive host */
  @Argument
  private String hiveHost = "hadoopminimstr003.prn1.facebook.com";

  /** Hive port */
  @Argument
  private int hivePort = 9083;

  /** Hive database */
  @Argument
  private String database = "default";

  /** Hive table */
  @Argument(required = true)
  private String table;

  /** Partition filter */
  @Argument
  private String partitionFilter;

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
}
