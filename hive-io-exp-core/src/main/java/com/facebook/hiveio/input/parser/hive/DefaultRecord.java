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

package com.facebook.hiveio.input.parser.hive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.record.HiveRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.google.common.base.Objects;

import java.util.Arrays;
import java.util.List;

/**
 * Single record from a Hive table. Used for both reading and writing.
 */
public class DefaultRecord implements HiveRecord {
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(DefaultRecord.class);

  /** Table schema - only set for writing */
  private final HiveTableSchema schema;

  /** Raw data for row */
  private final Object[] rowData;
  /** Partition data */
  private final String[] partitionValues;

  /**
   * Constructor
   *
   * @param schema HiveTableSchema
   * @param numColumns number of columns
   * @param partitionValues partition data
   */
  private DefaultRecord(HiveTableSchema schema, int numColumns, String[] partitionValues) {
    this.schema = schema;
    this.rowData = new Object[numColumns];
    this.partitionValues = partitionValues;
  }

  /**
   * Create a new record for reading
   *
   * @param numColumns number of column
   * @param partitionValues partition data
   * @return new DefaultRecord
   */
  public static DefaultRecord forReading(int numColumns, String[] partitionValues) {
    return new DefaultRecord(null, numColumns, partitionValues);
  }

  /**
   * Create a new record for writing
   *
   * @param schema Hive table schema
   * @return new DefaultRecord
   */
  public static DefaultRecord forWriting(HiveTableSchema schema) {
    return new DefaultRecord(schema, schema.numColumns(), new String[0]);
  }

  @Override
  public Object get(int index) {
    if (index < rowData.length) {
      return rowData[index];
    } else {
      return partitionValues[index - rowData.length];
    }
  }

  @Override
  public boolean getBoolean(int index) {
    Boolean v = (Boolean) get(index);
    return v == null ? false : v;
  }

  @Override
  public long getLong(int index) {
    Long v = (Long) get(index);
    return v == null ? Long.MIN_VALUE : v;
  }

  @Override
  public double getDouble(int index) {
    Double v = (Double) get(index);
    return v == null ? Double.NaN : v;
  }

  @Override
  public String getString(int index) {
    return (String) get(index);
  }

  @Override
  public boolean isNull(int index) {
    return get(index) == null;
  }

  @Override
  public List<Object> getAllColumns() {
    return Arrays.asList(rowData);
  }

  @Override
  public void set(int index, Object value) {
    rowData[index] = upgradeType(schema, index, value);
  }

  /**
   * Upgrade type if schema allows it.
   *
   * @param schema HiveTableSchema
   * @param index column index
   * @param value data to set
   * @return upgraded data
   */
  private static Object upgradeType(HiveTableSchema schema, int index, Object value) {
    if (value == null) {
      return null;        // pass through nulls
    }
    HiveType type = schema.columnType(index);
    return type.checkAndUpgrade(value);
  }

  @Override
  public int numColumns() {
    return rowData.length;
  }

  @Override
  public int numPartitionValues() {
    return partitionValues.length;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("numColumns", numColumns())
        .add("rowData", rowDataToString())
        .add("partitionData", partitionValues)
        .toString();
  }

  /**
   * String representation of row data for debugging
   * @return String dump of row data
   */
  private String rowDataToString() {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    for (int i = 0; i < rowData.length; ++i) {
      if (i > 0) {
        sb.append(',');
      }
      if (rowData[i] == null) {
        sb.append("(null)");
      } else {
        sb.append(rowData[i].getClass().getSimpleName());
        sb.append(":");
        sb.append(rowData[i].toString());
      }
    }
    sb.append(']');
    return sb.toString();
  }
}
