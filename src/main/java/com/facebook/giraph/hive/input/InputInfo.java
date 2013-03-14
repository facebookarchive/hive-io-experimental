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

package com.facebook.giraph.hive.input;

import org.apache.hadoop.io.Writable;

import com.facebook.giraph.hive.schema.HiveTableSchema;
import com.facebook.giraph.hive.schema.HiveApiTableSchema;
import com.facebook.giraph.hive.common.Writables;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Internal input information about Hive table to read
 */
class InputInfo implements Writable {
  /** Partitions to read */
  private final List<InputPartition> partitions = Lists.newArrayList();
  /** Schema of table */
  private final HiveTableSchema tableSchema;
  /** IDs of columns to read */
  private final List<Integer> columnIds;

  /**
   * Constructor
   */
  public InputInfo() {
    tableSchema = new HiveApiTableSchema();
    columnIds = Lists.newArrayList();
  }

  /**
   * Constructor
   *
   * @param tableSchema Hive table schema
   * @param columnIds IDs of columns to read
   */
  public InputInfo(HiveTableSchema tableSchema, List<Integer> columnIds) {
    this.tableSchema = tableSchema;
    this.columnIds = columnIds;
  }

  /**
   * Add a partition to read
   *
   * @param inputPartition Partition to add
   */
  public void addPartition(InputPartition inputPartition) {
    partitions.add(inputPartition);
  }

  public Iterable<InputPartition> getPartitions() {
    return partitions;
  }

  public HiveTableSchema getTableSchema() {
    return tableSchema;
  }

  public List<Integer> getColumnIds() {
    return columnIds;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    tableSchema.write(out);
    Writables.writeList(out, partitions);
    Writables.writeIntList(out, columnIds);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    tableSchema.readFields(in);
    partitions.clear();
    int numItems = in.readInt();
    for (int i = 0; i < numItems; ++i) {
      InputPartition ip = new InputPartition();
      ip.readFields(in);
      partitions.add(ip);
    }
    Writables.readIntList(in, columnIds);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("tableSchema", tableSchema)
        .add("partitions", partitions)
        .toString();
  }
}
