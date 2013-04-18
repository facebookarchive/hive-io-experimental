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

import com.facebook.hiveio.cmdline.BaseCmd;
import com.facebook.hiveio.cmdline.Defaults;
import io.airlift.command.Command;
import io.airlift.command.Option;

@Command(name = "tail", description = "Dump a Hive table")
public class TailerCmd extends BaseCmd {
  @Option(name = "--clustersFile")
  public String clustersFile;

  @Option(name = "--cluster")
  public String cluster;

  @Option(name = "--metastoreHost")
  public String metastoreHost;

  @Option(name = "--metastorePort")
  public int metastorePort = Defaults.METASTORE_PORT;

  @Option(name = "--database")
  public String database = Defaults.DATABASE;

  @Option(name = {"-t", "--table"})
  public String table;

  @Option(name = {"-f", "--partitionFilter"})
  public String partitionFilter = Defaults.PARTITION_FILTER;

  @Option(name = {"-l", "--limit"})
  public long limit = Long.MAX_VALUE;

  @Option(name = "--threads")
  public int threads = 1;

  @Option(name = "--separator")
  public String separator = Defaults.SEPARATOR;

  @Option(name = "--requestNumSplits")
  public int requestNumSplits = 0;

  @Option(name = "--requestSplitsPerThread")
  public int requestSplitsPerThread = 3;

  @Option(name = "--metricsUpdatePeriodRows")
  public int metricsUpdatePeriodRows = 1000;

  @Option(name = "--metricsPrintPeriodSecs")
  public int metricsPrintPeriodSecs = 0;

  @Override public void execute() throws Exception {
    new Tailer().run(this);
  }
}
