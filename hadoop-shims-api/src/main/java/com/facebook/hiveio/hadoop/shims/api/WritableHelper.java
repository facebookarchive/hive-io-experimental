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
package com.facebook.hiveio.hadoop.shims.api;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * Helper for WritableShims
 */
public interface WritableHelper {
  /**
   * Read a string from input
   *
   * @param in DataInput
   * @return String read in
   */
  String readString(DataInput in);

  /**
   * Writes a string to output
   *
   * @param out DataOutput
   * @param str String to write
   */
  void writeString(DataOutput out, String str);
}
