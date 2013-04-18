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

package com.facebook.hiveio.common;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

/**
 * Wrap Progressable with Reporter interface
 */
public class ProgressReporter implements Reporter {
  /** For progress() calls */
  private Progressable progressable;

  /**
   * Constructor
   * @param progressable Progressable to use
   */
  public ProgressReporter(Progressable progressable) {
    this.progressable = progressable;
  }

  @Override
  public void setStatus(String status) {
  }

  @Override
  public Counters.Counter getCounter(Enum<?> name) {
    return NULL.getCounter(name);
  }

  @Override
  public Counters.Counter getCounter(String group, String name) {
    return NULL.getCounter(group, name);
  }

  @Override
  public void incrCounter(Enum<?> key, long amount) {
  }

  @Override
  public void incrCounter(String group, String counter, long amount) {
  }

  @Override
  public InputSplit getInputSplit() {
    return NULL.getInputSplit();
  }

  @Override
  public void progress() {
    progressable.progress();
  }
}
