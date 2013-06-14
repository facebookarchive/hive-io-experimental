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

import io.airlift.command.Option;

/**
 * Options for Hadoop input splits
 */
public class SplitOptions {
  // CHECKSTYLE: stop VisibilityModifier
  /** number of splits */
  @Option(name = "--request-num-splits", description = "Number of splits to request")
  public int requestNumSplits = 0;

  /** splits per thread */
  @Option(name = "--request-splits-per-thread", description = "Number of splits per thread")
  public int requestSplitsPerThread = 3;
  // CHECKSTYLE: resume VisibilityModifier

  /**
   * Compute number of splits
   *
   * @param threads number of threads
   */
  public void compute(int threads) {
    if (requestNumSplits == 0) {
      requestNumSplits = threads * requestSplitsPerThread;
    }
  }
}
