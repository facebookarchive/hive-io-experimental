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

package com.facebook.hiveio.record;

/**
 * Hive record representing a row for reading.
 */
public interface HiveReadableRecord {
  /**
   * Get column at given index.
   *
   * Regular data columns from the tables should always be placed first, and then
   * partition value columns.
   *
   * If you know the type of the column and it is a primitive you should use
   * one of the calls below as it will likely be more efficient.
   *
   * @param index column index
   * @return Object for column
   */
  Object get(int index);

  /**
   * Get boolean column at given index
   * @param index column index
   * @return boolean at index
   */
  boolean getBoolean(int index);

  /**
   * Get long column at given index
   * @param index column index
   * @return long at index
   */
  long getLong(int index);

  /**
   * Get double column at given index
   * @param index column index
   * @return double at index
   */
  double getDouble(int index);

  /**
   * Get String column at given index.
   * Note that partition values are all strings.
   *
   * @param index column index
   * @return String at index
   */
  String getString(int index);

  /**
   * Check if value at column is null
   * @param index column index
   * @return true if value at at column is null
   */
  boolean isNull(int index);
}
