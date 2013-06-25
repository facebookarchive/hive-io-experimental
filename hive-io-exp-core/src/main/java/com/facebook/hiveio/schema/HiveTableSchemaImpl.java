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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import com.facebook.hiveio.common.HiveTableDesc;
import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.common.HiveUtils;
import com.facebook.hiveio.common.Writables;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.facebook.hiveio.common.HiveUtils.FIELD_SCHEMA_NAME_GETTER;
import static com.google.common.collect.Lists.transform;

/**
 * Schema for a Hive table
 */
public class HiveTableSchemaImpl implements HiveTableSchema {
  /** Hive table name */
  private final HiveTableDesc tableName;

  /** Partition keys */
  private final Map<String, Integer> partitionPositions;
  /** Positions of columns in the row */
  private final Map<String, Integer> columnPositions;

  /** Column types */
  private HiveType[] hiveTypes;

  /** Number of columns. Not serialized */
  private int numColumns;
  /** Number of partition keys. Not serialized */
  private int numPartitionKeys;

  /**
   * Constructor
   */
  public HiveTableSchemaImpl() {
    tableName = new HiveTableDesc("_unknown_");
    partitionPositions = Maps.newHashMap();
    columnPositions = Maps.newHashMap();
    hiveTypes = new HiveType[0];
  }

  /**
   * Constructor
   *
   * @param tableName Name of Hive table
   * @param partitionPositions Partition keys
   * @param columnPositions Positions of columns in row
   * @param hiveTypes Types of columns
   */
  public HiveTableSchemaImpl(HiveTableDesc tableName,
                             Map<String, Integer> partitionPositions,
                             Map<String, Integer> columnPositions,
                             HiveType[] hiveTypes) {
    this.tableName = tableName;
    this.partitionPositions = partitionPositions;
    this.columnPositions = columnPositions;
    this.hiveTypes = hiveTypes;
    numColumns = sizeFromIndexes(columnPositions);
    numPartitionKeys = sizeFromIndexes(partitionPositions);
  }
  /**
   * Create from a Hive table
   *
   * @param conf Configuration
   * @param table Hive table
   * @return Schema
   */
  public static HiveTableSchemaImpl fromTable(Configuration conf, Table table) {
    int index = 0;

    StorageDescriptor storageDescriptor = table.getSd();
    HiveType[] hiveTypes = HiveUtils.columnTypes(conf, storageDescriptor);

    List<String> columnNames = transform(storageDescriptor.getCols(), FIELD_SCHEMA_NAME_GETTER);
    Map<String, Integer> columnToIndex = Maps.newHashMap();
    for (String columnName : columnNames) {
      columnToIndex.put(columnName, index++);
    }

    List<String> partitionNames = transform(table.getPartitionKeys(), FIELD_SCHEMA_NAME_GETTER);
    Map<String, Integer> partitionToIndex = Maps.newHashMap();
    for (String partitionName : partitionNames) {
      partitionToIndex.put(partitionName, index++);
    }

    HiveTableDesc hiveTableDesc = new HiveTableDesc(table.getDbName(), table.getTableName());
    return new HiveTableSchemaImpl(hiveTableDesc, partitionToIndex, columnToIndex, hiveTypes);
  }

  /**
   * Compute number of columns from the data.
   * @param columnPositions column positions to compute on
   * @return number of columns
   */
  private static int sizeFromIndexes(Map<String, Integer> columnPositions) {
    if (columnPositions.isEmpty()) {
      return 0;
    }
    return Ordering.natural().max(columnPositions.values()) + 1;
  }

  @Override
  public HiveTableDesc getTableDesc() {
    return tableName;
  }

  @Override
  public int numColumns() {
    return numColumns;
  }

  @Override
  public int numPartitionKeys() {
    return numPartitionKeys;
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

  @Override public HiveType columnType(int columnIndex) {
    Preconditions.checkElementIndex(columnIndex, hiveTypes.length);
    return hiveTypes[columnIndex];
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Writables.writeStrIntMap(out, partitionPositions);
    Writables.writeStrIntMap(out, columnPositions);
    tableName.write(out);
    Writables.writeEnumArray(out, hiveTypes);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Writables.readStrIntMap(in, partitionPositions);
    Writables.readStrIntMap(in, columnPositions);
    numColumns = sizeFromIndexes(columnPositions);
    tableName.readFields(in);
    hiveTypes = Writables.readEnumArray(in, HiveType.class);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("tableName", tableName)
        .add("partitionKeys", partitionPositions)
        .add("columnPositions", columnPositions)
        .add("hiveTypes", hiveTypes)
        .toString();
  }
}
