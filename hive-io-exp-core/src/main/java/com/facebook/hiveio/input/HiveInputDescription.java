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

package com.facebook.hiveio.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.thrift.TException;

import com.facebook.hiveio.common.HiveTableDesc;
import com.facebook.hiveio.common.MetastoreDesc;
import com.facebook.hiveio.common.Writables;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Description of hive table to read from.
 */
public class HiveInputDescription implements Writable {
  /** Metastore to use. Optional. */
  private MetastoreDesc metastoreDesc = new MetastoreDesc();
  /** Hive table */
  private HiveTableDesc tableDesc = new HiveTableDesc();
  /** Columns to read in table */
  private List<String> columns = Lists.newArrayList();
  /** Filter for which partitions to read from  */
  private String partitionFilter = "";
  /** Number of splits per matched partition desired */
  private int numSplits;

  /**
   * Get columns to read
   *
   * @return list of columns to read
   */
  public List<String> getColumns() {
    return columns;
  }

  /**
   * Add a column to read
   *
   * @param column String name of column
   * @return this
   */
  public HiveInputDescription addColumn(String column) {
    columns.add(column);
    return this;
  }

  /**
   * Set columns to read
   *
   * @param columns list of columns to read
   * @return this
   */
  public HiveInputDescription setColumns(List<String> columns) {
    this.columns = columns;
    return this;
  }

  /**
   * Check if we have columns specified
   *
   * @return true if we have columns
   */
  public boolean hasColumns() {
    return columns != null && !columns.isEmpty();
  }

  public HiveTableDesc getTableDesc() {
    return tableDesc;
  }

  public void setTableDesc(HiveTableDesc hiveTableDesc) {
    this.tableDesc = hiveTableDesc;
  }

  /**
   * Get partition filter string
   *
   * @return partition filter
   */
  public String getPartitionFilter() {
    return partitionFilter;
  }

  /**
   * Set partition filter
   *
   * @param partitionFilter HQL partition filter
   * @return this
   */
  public HiveInputDescription setPartitionFilter(String partitionFilter) {
    this.partitionFilter = partitionFilter;
    return this;
  }

  /**
   * Do we have a partition filter
   *
   * @return true if we have a partition filter
   */
  public boolean hasPartitionFilter() {
    return partitionFilter != null && !partitionFilter.isEmpty();
  }

  /**
   * Get desired number of splits
   *
   * @return number of splits
   */
  public int getNumSplits() {
    return numSplits;
  }

  /**
   * Set number of splits
   *
   * @param numSplits number of splits
   * @return this
   */
  public HiveInputDescription setNumSplits(int numSplits) {
    this.numSplits = numSplits;
    return this;
  }

  public MetastoreDesc getMetastoreDesc() {
    return metastoreDesc;
  }

  /**
   * Make metastore client
   *
   * @param conf Configuration
   * @return Thrift Metastore client
   * @throws TException
   */
  public ThriftHiveMetastore.Iface metastoreClient(Configuration conf) throws TException {
    return metastoreDesc.makeClient(conf);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    metastoreDesc.write(out);
    tableDesc.write(out);
    Writables.writeStringList(out, columns);
    WritableUtils.writeString(out, partitionFilter);
    out.writeInt(numSplits);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    metastoreDesc.readFields(in);
    tableDesc.readFields(in);
    Writables.readStringList(in, columns);
    partitionFilter = WritableUtils.readString(in);
    numSplits = in.readInt();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("metastoreDesc", metastoreDesc)
        .add("tableDesc", tableDesc)
        .add("columns", columns)
        .add("partitionFilter", partitionFilter)
        .add("numSplits", String.valueOf(numSplits))
        .toString();
  }
}
