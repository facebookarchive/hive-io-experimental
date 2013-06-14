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
package com.facebook.hiveio.options;

import org.apache.hadoop.conf.Configuration;

import com.facebook.hiveio.input.parser.Parsers;
import com.facebook.hiveio.input.parser.RecordParser;
import io.airlift.command.Option;

/**
 * Options for input table
 */
public class InputTableOptions extends TableOptions {
  // CHECKSTYLE: stop VisibilityModifier
  /** partition filter */
  @Option(name = { "-f", "--partition-filter" }, description = "Partition filter")
  public String partitionFilter = Defaults.PARTITION_FILTER;

  /** parser to use */
  @Option(name = "--parser", description = "Force Input parser to use")
  public String parser;
  // CHECKSTYLE: resume VisibilityModifier

  /**
   * Process Configuration
   *
   * @param conf Configuration
   */
  public void process(Configuration conf) {
    processParser(conf);
  }

  /**
   * Process parser
   *
   * @param conf Configuration
   */
  private void processParser(Configuration conf) {
    if (parser != null) {
      Class<?> klass;
      try {
        klass = Class.forName(parser);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException(e);
      }
      Class<? extends RecordParser> parserClass = klass.asSubclass(RecordParser.class);
      Parsers.FORCE_PARSER.set(conf, parserClass);
    }
  }
}
