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

package com.facebook.hiveio.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.thrift.TException;

import com.facebook.hiveio.common.HiveTableName;
import com.facebook.hiveio.common.MetastoreDesc;
import com.facebook.hiveio.common.Writables;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Description of Hive table to write to
 */
public class HiveOutputDescription implements Writable {
  /** Metastore to use. Optional. */
  private MetastoreDesc metastoreDesc = new MetastoreDesc();
  /** Hive database */
  private String dbName = "default";
  /** Hive table */
  private String tableName = "";
  /** Partition to write to */
  private Map<String, String> partitionValues = Maps.newHashMap();

  public String getDbName() {
    return dbName;
  }

  /**
   * Set database name
   * @param dbName database name
   * @return this
   */
  public HiveOutputDescription setDbName(String dbName) {
    this.dbName = dbName;
    return this;
  }

  /**
   * Check if we have a database name
   * @return true if we have a database name
   */
  public boolean hasDbName() {
    return dbName != null && !dbName.isEmpty();
  }

  public String getTableName() {
    return tableName;
  }

  /**
   * Set table name
   * @param tableName Hive table name
   * @return this
   */
  public HiveOutputDescription setTableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  /**
   * Check if we have a table name
   * @return true if table name set
   */
  public boolean hasTableName() {
    return tableName != null && !tableName.isEmpty();
  }

  /**
   * Make hive table name from this
   * @return HiveTableName
   */
  public HiveTableName  hiveTableName() {
    return new HiveTableName(dbName, tableName);
  }

  public Map<String, String> getPartitionValues() {
    return partitionValues;
  }

  /**
   * Get size of partition data
   * @return number of partition items
   */
  public int numPartitionValues() {
    return partitionValues.size();
  }

  /**
   * Check if we have partition data
   * @return true if have partition data
   */
  public boolean hasPartitionValues() {
    return partitionValues != null && !partitionValues.isEmpty();
  }

  /**
   * Set partition data
   * @param partitionValues partition data
   * @return this
   */
  public HiveOutputDescription setPartitionValues(
      Map<String, String> partitionValues) {
    this.partitionValues = partitionValues;
    return this;
  }

  /**
   * Put partition data
   *
   * @param key partition key
   * @param value partition value
   * @return this
   */
  public HiveOutputDescription putPartitionValue(String key, String value) {
    this.partitionValues.put(key, value);
    return this;
  }

  public MetastoreDesc getMetastoreDesc() {
    return metastoreDesc;
  }

  /**
   * Get client to Metastore
   *
   * @param conf Configuration
   * @return Thrift metastore client
   * @throws TException
   */
  public ThriftHiveMetastore.Iface metastoreClient(Configuration conf) throws TException {
    return metastoreDesc.makeClient(conf);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    metastoreDesc.write(out);
    WritableUtils.writeString(out, dbName);
    WritableUtils.writeString(out, tableName);
    Writables.writeStrStrMap(out, partitionValues);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    metastoreDesc.readFields(in);
    dbName = WritableUtils.readString(in);
    tableName = WritableUtils.readString(in);
    Writables.readStrStrMap(in, partitionValues);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("dbName", dbName)
        .add("tableName", tableName)
        .add("partitionValues", partitionValues)
        .toString();
  }
}
