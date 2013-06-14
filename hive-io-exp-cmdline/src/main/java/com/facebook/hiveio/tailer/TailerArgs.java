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
import com.facebook.hiveio.options.MetricsOptions;
import com.facebook.hiveio.options.MultiThreadOptions;
import com.facebook.hiveio.options.NamespaceOptions;
import com.facebook.hiveio.options.ParserOptions;
import com.facebook.hiveio.options.SplitOptions;
import io.airlift.command.Option;

import javax.inject.Inject;

/**
 * Arguments for tailer command
 */
public class TailerArgs {
  // CHECKSTYLE: stop VisibilityModifier
  /** namespace */
  @Inject public NamespaceOptions namespace = new NamespaceOptions();
  /** metastore */
  @Inject public MetastoreOptions metastore = new MetastoreOptions();
  /** input table */
  @Inject public InputTableOptions inputTable = new InputTableOptions();
  /** multi threaded */
  @Inject public MultiThreadOptions multiThread = new MultiThreadOptions();
  /** parser */
  @Inject public ParserOptions parser = new ParserOptions();
  /** input splits */
  @Inject public SplitOptions splits = new SplitOptions();
  /** metrics */
  @Inject public MetricsOptions metricsOpts = new MetricsOptions();

  /** limit */
  @Option(name = { "-l", "--limit" }, description = "Limit on number of rows to process")
  public long limit = Long.MAX_VALUE;

  /** separator */
  @Option(name = "--separator", description = "Separator between columns")
  public String separator = Defaults.SEPARATOR;

  /** buffre flush period */
  @Option(name = "--record-buffer-flush",
      description = "How many records to buffer before printing")
  public int recordBufferFlush = 1;

  /** where to append stats */
  @Option(name = "--append-stats-to", description = "Append final stats to a file")
  public String appendStatsTo;
  // CHECKSTYLE: resume VisibilityModifier

  /** Process options */
  public void process() {
    metricsOpts.process();
  }
}
