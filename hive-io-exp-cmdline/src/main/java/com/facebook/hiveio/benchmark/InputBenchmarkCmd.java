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

import com.facebook.hiveio.cmdline.BaseCmd;
import com.facebook.hiveio.cmdline.Defaults;
import io.airlift.command.Command;
import io.airlift.command.Option;

@Command(name = "input-benchmark", description = "Benchmark for Input")
public class InputBenchmarkCmd extends BaseCmd {
  /** Hive host */
  @Option(name = {"-h", "--host"}, description = "Hive Metastore host")
  public String hiveHost = Defaults.METASTORE_HOST;

  /** Hive port */
  @Option(name = {"-p", "--port"}, description = "Hive Metatstore port")
  public int hivePort = Defaults.METASTORE_PORT;

  /** Hive database */
  @Option(name = "--database", description = "Hive database to use")
  public String database = Defaults.DATABASE;

  /** Hive table */
  @Option(name = {"-t", "--table"}, description = "Hive table to query")
  public String table = "inference_sims";

  /** Partition filter */
  @Option(name = {"-f", "--partitionFilter"}, description = "Input partition filter")
  public String partitionFilter = "ds='2013-01-01' and feature='test_nz'";

  /** Whether to track metrics */
  @Option(name = "--trackMetrics", description = "Track metrics while running")
  public boolean trackMetrics = false;

  /** Every how many splits to print */
  @Option(name = "--splitPrintPeriod", description = "Every how many splits to print")
  public int splitPrintPeriod = 3;

  /** Every how many records in a split to print */
  @Option(name = "--recordPrintPeriod")
  public int recordPrintPeriod = 1000000;

  @Override public void execute() throws Exception {
    new InputBenchmark().run(this);
  }
}
