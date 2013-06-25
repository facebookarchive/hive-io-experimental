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

import com.facebook.hiveio.common.HiveTableDesc;
import com.facebook.hiveio.common.HiveType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A constructable {@link HiveTableSchema} for testing.
 */
public class TestSchema implements HiveTableSchema {
  /** Column name to index */
  private final Map<String, Integer> nameToIndex = Maps.newHashMap();
  /** Column types */
  private final List<HiveType> types = Lists.newArrayList();
  /** Table description */
  private final HiveTableDesc tableDesc = new HiveTableDesc();
  /** Number of partitions */
  private int numPartitions = 0;
  /** Max column index */
  private int maxIndex = -1;

  @Override public HiveType columnType(int columnIndex) {
    return types.get(columnIndex);
  }

  @Override public HiveTableDesc getTableDesc() {
    return tableDesc;
  }

  @Override public int positionOf(String columnOrPartitionKeyName) {
    return nameToIndex.get(columnOrPartitionKeyName);
  }

  @Override public int numColumns() {
    return types.size();
  }

  @Override public int numPartitionKeys() {
    return numPartitions;
  }

  @Override public void write(DataOutput out) throws IOException {
    throw new IllegalStateException();
  }

  @Override public void readFields(DataInput in) throws IOException {
    throw new IllegalStateException();
  }

  /**
   * Builder for TestSchema
   */
  public static class Builder {
    /** The schema being built */
    private final TestSchema schema = new TestSchema();

    /**
     * Add a column to end of table
     *
     * @param name String column name
     * @param type HiveType
     * @return this
     */
    public Builder addColumn(String name, HiveType type) {
      return addColumn(name, schema.maxIndex + 1, type);
    }

    /**
     * Add a column
     *
     * @param name String column name
     * @param index column index
     * @param type HiveType
     * @return this
     */
    public Builder addColumn(String name, int index, HiveType type) {
      Preconditions.checkArgument(schema.types.size() <= index);
      while (schema.types.size() < index) {
        schema.types.add(null);
      }
      schema.types.add(type);
      schema.nameToIndex.put(name, index);
      schema.maxIndex = Math.max(schema.maxIndex, index);
      return this;
    }

    /**
     * Set number of partitions
     *
     * @param numPartitions num partitions
     * @return this
     */
    public Builder setNumPartitions(int numPartitions) {
      schema.numPartitions = numPartitions;
      return this;
    }

    /**
     * Set table name
     *
     * @param tableName the table name
     * @return this
     */
    public Builder setTableName(String tableName) {
      schema.tableDesc.setTableName(tableName);
      return this;
    }

    /**
     * Build the schema
     *
     * @return {@link TestSchema}
     */
    public TestSchema build() {
      return schema;
    }
  }

  /**
   * Create a new builder
   *
   * @return {@link Builder}
   */
  public static Builder builder() {
    return new Builder();
  }
}
