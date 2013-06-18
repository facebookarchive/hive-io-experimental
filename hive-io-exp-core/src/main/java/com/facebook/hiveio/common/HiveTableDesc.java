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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Name of a table in Hive
 */
public class HiveTableDesc implements Writable {
  /** defautl database */
  private static final String DEFAULT_DATABASE = "default";

  /** Database where table is located */
  private String databaseName;
  /** Name of the table */
  private String tableName;

  /** Empty constructor */
  public HiveTableDesc() {
    databaseName = DEFAULT_DATABASE;
    tableName = "_unknown_";
  }

  /**
   * Constructor
   *
   * @param tableName table
   */
  public HiveTableDesc(String tableName) {
    this.databaseName = DEFAULT_DATABASE;
    this.tableName = tableName;
  }

  /**
   * Constructor
   *
   * @param databaseName database
   * @param tableName table
   */
  public HiveTableDesc(String databaseName, String tableName) {
    this.databaseName = databaseName;
    this.tableName = tableName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * Convert to string in dot format
   *
   * @return database.name
   */
  public String dotString() {
    return databaseName + "." + tableName;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    databaseName = WritableUtils.readString(in);
    tableName = WritableUtils.readString(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, databaseName);
    WritableUtils.writeString(out, tableName);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("databaseName", databaseName)
        .add("tableName", tableName)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HiveTableDesc that = (HiveTableDesc) o;

    return Objects.equal(databaseName, that.databaseName) &&
        Objects.equal(tableName, that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(databaseName, tableName);
  }
}
