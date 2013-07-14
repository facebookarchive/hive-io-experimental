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
import com.facebook.hiveio.input.parser.Records;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.record.HiveRecord;
import com.facebook.hiveio.record.HiveWritableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.google.common.base.Objects;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
   * @param schema Hive table schema
   * @param partitionValues partition data
   * @return new DefaultRecord
   */
  public static HiveReadableRecord forReading(HiveTableSchema schema, String[] partitionValues) {
    return new DefaultRecord(schema, schema.numColumns(), partitionValues);
  }

  /**
   * Create a new record for writing
   *
   * @param schema Hive table schema
   * @return new DefaultRecord
   */
  public static HiveWritableRecord forWriting(HiveTableSchema schema) {
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
  public Object get(int index, HiveType hiveType) {
    return Records.get(this, index, hiveType);
  }

  @Override
  public boolean getBoolean(int index) {
    Records.verifyType(index, HiveType.BOOLEAN, schema);
    Boolean v = (Boolean) get(index);
    return v == null ? false : v;
  }

  @Override
  public byte getByte(int index) {
    Records.verifyType(index, HiveType.BYTE, schema);
    Byte v = (Byte) get(index);
    return v == null ? Byte.MIN_VALUE : v;
  }

  @Override
  public short getShort(int index) {
    Records.verifyType(index, HiveType.SHORT, schema);
    Short v = (Short) get(index);
    return v == null ? Short.MIN_VALUE : v;
  }

  @Override
  public int getInt(int index) {
    Records.verifyType(index, HiveType.INT, schema);
    Integer v = (Integer) get(index);
    return v == null ? Integer.MIN_VALUE : v;
  }

  @Override
  public long getLong(int index) {
    Records.verifyType(index, HiveType.LONG, schema);
    Long v = (Long) get(index);
    return v == null ? Long.MIN_VALUE : v;
  }

  @Override
  public float getFloat(int index) {
    Records.verifyType(index, HiveType.FLOAT, schema);
    Float v = (Float) get(index);
    return v == null ? Float.MIN_VALUE : v;
  }

  @Override
  public double getDouble(int index) {
    Records.verifyType(index, HiveType.DOUBLE, schema);
    Double v = (Double) get(index);
    return v == null ? Double.NaN : v;
  }

  @Override
  public String getString(int index) {
    Records.verifyType(index, HiveType.STRING, schema);
    return (String) get(index);
  }

  @Override
  public <K, V> Map<K, V> getMap(int index) {
    Records.verifyMapLike(index, schema);
    return (Map<K, V>) get(index);
  }

  @Override
  public <T> List<T> getList(int index) {
    Records.verifyType(index, HiveType.LIST, schema);
    return (List<T>) get(index);
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

  @Override
  public void set(int index, Object value, HiveType hiveType) {
    Records.verifyType(index, hiveType, schema);
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

  /**
   * Set data for column
   *
   * @param index column index
   * @param value data
   * @param expectedType HiveType expected
   */
  private void setImpl(int index, Object value, HiveType expectedType) {
    Records.verifyType(index, expectedType, schema);
    rowData[index] = value;
  }

  @Override
  public void setBoolean(int index, boolean value) {
    setImpl(index, value, HiveType.BOOLEAN);
  }

  @Override
  public void setByte(int index, byte value) {
    setImpl(index, value, HiveType.BYTE);
  }

  @Override
  public void setShort(int index, short value) {
    setImpl(index, value, HiveType.SHORT);
  }

  @Override
  public void setInt(int index, int value) {
    setImpl(index, value, HiveType.INT);
  }

  @Override
  public void setLong(int index, long value) {
    setImpl(index, value, HiveType.LONG);
  }

  @Override
  public void setFloat(int index, float value) {
    setImpl(index, value, HiveType.FLOAT);
  }

  @Override
  public void setDouble(int index, double value) {
    setImpl(index, value, HiveType.DOUBLE);
  }

  @Override
  public void setString(int index, String value) {
    setImpl(index, value, HiveType.STRING);
  }

  @Override
  public void setMap(int index, Map data) {
    Records.verifyMapLike(index, schema);
    rowData[index] = data;
  }

  @Override
  public void setList(int index, List data) {
    setImpl(index, data, HiveType.LIST);
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
  public HiveType columnType(int index) {
    return schema.columnType(index);
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
