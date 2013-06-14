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

import com.facebook.hiveio.options.BaseCmd;
import com.facebook.hiveio.options.InputTableOptions;
import com.facebook.hiveio.options.MetastoreOptions;
import io.airlift.command.Command;
import io.airlift.command.Option;

import javax.inject.Inject;

/**
 * Input benchmark
 */
@Command(name = "input-benchmark", description = "Benchmark for Input")
public class InputBenchmarkCmd extends BaseCmd {
  // CHECKSTYLE: stop VisibilityModifier
  /** metastore options */
  @Inject public MetastoreOptions metastoreOpts = new MetastoreOptions();

  /** table options */
  @Inject public InputTableOptions tableOpts = new InputTableOptions();

  /** Whether to track metrics */
  @Option(name = "--trackMetrics", description = "Track metrics while running")
  public boolean trackMetrics = false;

  /** Every how many splits to print */
  @Option(name = "--splitPrintPeriod", description = "Every how many splits to print")
  public int splitPrintPeriod = 3;

  /** Every how many records in a split to print */
  @Option(name = "--recordPrintPeriod")
  public int recordPrintPeriod = 1000000;
  // CHECKSTYLE: resume VisibilityModifier

  @Override public void execute() throws Exception {
    new InputBenchmark().run(this);
  }
}
