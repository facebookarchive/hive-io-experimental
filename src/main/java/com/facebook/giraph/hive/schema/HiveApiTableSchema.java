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

package com.facebook.giraph.hive.impl;

import org.apache.hadoop.hive.metastore.api.Table;

import com.facebook.giraph.hive.common.Writables;
import com.facebook.giraph.hive.schema.HiveTableSchema;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.facebook.giraph.hive.common.HiveUtils.FIELD_SCHEMA_NAME_GETTER;
import static com.google.common.collect.Lists.transform;

/**
 * Schema for a Hive table
 */
public class HiveApiTableSchema implements HiveTableSchema {
  /** Partition keys */
  private final Map<String, Integer> partitionPositions;
  /** Positions of columns in the row */
  private final Map<String, Integer> columnPositions;

  /** Number of columns. Not serialized */
  private int numColumns;

  /**
   * Constructor
   */
  public HiveApiTableSchema() {
    partitionPositions = Maps.newHashMap();
    columnPositions = Maps.newHashMap();
  }

  /**
   * Constructor
   *
   * @param partitionPositions Partition keys
   * @param columnPositions Positions of columns in row
   */
  public HiveApiTableSchema(Map<String, Integer> partitionPositions,
                            Map<String, Integer> columnPositions) {
    this.partitionPositions = partitionPositions;
    this.columnPositions = columnPositions;
    numColumns = computeNumColumns(columnPositions);
  }
  /**
   * Create from a Hive table
   *
   * @param table Hive table
   * @return Schema
   */
  public static HiveApiTableSchema fromTable(Table table) {
    int index = 0;

    List<String> columnNames = transform(table.getSd().getCols(), FIELD_SCHEMA_NAME_GETTER);
    Map<String, Integer> columnToIndex = Maps.newHashMap();
    for (String columnName : columnNames) {
      columnToIndex.put(columnName, index++);
    }

    List<String> partitionNames = transform(table.getPartitionKeys(), FIELD_SCHEMA_NAME_GETTER);
    Map<String, Integer> partitionToIndex = Maps.newHashMap();
    for (String partitionName : partitionNames) {
      partitionToIndex.put(partitionName, index++);
    }

    return new HiveApiTableSchema(partitionToIndex, columnToIndex);
  }

  /**
   * Compute number of columns from the data.
   * @param columnPositions column positions to compute on
   * @return number of columns
   */
  private static int computeNumColumns(Map<String, Integer> columnPositions) {
    return Ordering.natural().max(columnPositions.values()) + 1;
  }

  @Override
  public int numColumns() {
    return numColumns;
  }

  @Override
  public int positionOf(String columnOrPartitionKeyName) {
    Integer index = columnPositions.get(columnOrPartitionKeyName);
    if (index == null) {
      index = partitionPositions.get(columnOrPartitionKeyName);
      if (index == null) {
        throw new IllegalArgumentException("Column or partition " +
            columnOrPartitionKeyName + " not found in schema " + this);
      }
    }
    return index;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Writables.writeStrIntMap(out, partitionPositions);
    Writables.writeStrIntMap(out, columnPositions);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Writables.readStrIntMap(in, partitionPositions);
    Writables.readStrIntMap(in, columnPositions);
    numColumns = computeNumColumns(columnPositions);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("partitionKeys", partitionPositions)
        .add("columnPositions", columnPositions)
        .toString();
  }
}
