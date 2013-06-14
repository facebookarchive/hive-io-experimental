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
 * Options for multi-threaded
 */
public class MultiThreadOptions {
  // CHECKSTYLE: stop VisibilityModifier
  /** Number of threads to use */
  @Option(name = "--threads", description = "Number of threads to use")
  public int threads = 1;
  // CHECKSTYLE: resume VisibilityModifier

  public boolean isMultiThreaded() {
    return threads > 1;
  }

  public boolean isSingleThreaded() {
    return threads == 1;
  }
}
