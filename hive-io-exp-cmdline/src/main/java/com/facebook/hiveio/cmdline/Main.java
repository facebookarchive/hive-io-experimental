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
package com.facebook.hiveio.cmdline;

import com.facebook.hiveio.benchmark.InputBenchmarkCmd;
import com.facebook.hiveio.output.OutputCmd;
import com.facebook.hiveio.tailer.TailerCmd;
import io.airlift.command.Cli;
import io.airlift.command.Help;

/**
 * Command line entry point
 */
public class Main {
  /** Don't construct */
  private Main() { }

  /**
   * Entry point for command line
   *
   * @param args command line args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("hiveio");
    builder.withDefaultCommand(Help.class);
    builder.withCommand(Help.class);
    builder.withCommand(InputBenchmarkCmd.class);
    builder.withCommand(TailerCmd.class);
    builder.withCommand(ConfOptionsCmd.class);
    builder.withCommand(OutputCmd.class);
    Cli<Runnable> cli = builder.build();
    cli.parse(args).run();
  }
}
