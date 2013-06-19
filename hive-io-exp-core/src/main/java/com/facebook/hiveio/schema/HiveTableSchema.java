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

package com.facebook.hiveio.schema;

import org.apache.hadoop.io.Writable;

import com.facebook.hiveio.common.HiveTableDesc;
import com.facebook.hiveio.common.HiveType;

/**
 * Schema for a Hive table
 */
public interface HiveTableSchema extends Writable {
  /**
   * Get Hive table name
   *
   * @return Hive table name
   */
  HiveTableDesc getTableDesc();

  /**
   * Get index of a column or a paritition key
   *
   * Regular data columns from the tables should always be placed first, and then
   * partition value columns.
   *
   * @param columnOrPartitionKeyName Name of column or partition key
   * @return Integer index of column or partition key, or -1
   */
  int positionOf(String columnOrPartitionKeyName);

  /**
   * Get HiveType of column
   *
   * @param columnIndex column
   * @return HiveType
   */
  HiveType columnType(int columnIndex);

  /**
   * Get number of columns in table
   *
   * @return Number of columns in table
   */
  int numColumns();

  /**
   * Get number of partition keys for this table
   *
   * @return number of partition keys
   */
  int numPartitionKeys();
}
