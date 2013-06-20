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
package com.facebook.hiveio.input.parser.array;

import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.common.NativeType;
import com.facebook.hiveio.input.parser.Records;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.google.common.base.Objects;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Record backed by arrays
 */
public class ArrayRecord implements HiveReadableRecord {
  /** Number of columns */
  private final int numColumns;

  // Note that partition data and column data are stored together, with column
  // data coming before partition values.
  /** Hive types */
  private final HiveType[] hiveTypes;
  /** booleans */
  private final boolean[] booleans;
  /** longs */
  private final long[] longs;
  /** doubles */
  private final double[] doubles;
  /** strings */
  private final String[] strings;
  /** objects */
  private final Object[] objects;
  /** nulls */
  private final boolean[] nulls;

  /**
   * Constructor
   *
   * @param numColumns number of columns
   * @param partitionValues partition data
   * @param hiveTypes hive types
   */
  public ArrayRecord(int numColumns, String[] partitionValues, HiveType[] hiveTypes) {
    this.numColumns = numColumns;
    this.hiveTypes = hiveTypes;

    final int size = numColumns + partitionValues.length;
    this.booleans = new boolean[size];
    this.longs = new long[size];
    this.doubles = new double[size];
    this.strings = new String[size];
    this.objects = new Object[size];
    this.nulls = new boolean[size];

    for (int partIndex = 0; partIndex < partitionValues.length; ++partIndex) {
      strings[partIndex + numColumns] = partitionValues[partIndex];
    }
  }

  @Override
  public int numColumns() {
    return numColumns;
  }

  @Override
  public int numPartitionValues() {
    return booleans.length - numColumns;
  }

  public int getSize() {
    return booleans.length;
  }

  /** Reset data */
  public void reset() {
    Arrays.fill(nulls, false);
  }

  /**
   * Get HiveType for column
   *
   * @param index column index
   * @return HiveType
   */
  public HiveType getHiveType(int index) {
    return hiveTypes[index];
  }

  /**
   * Get Hive NativeType for column
   *
   * @param index column index
   * @return NativeType
   */
  public NativeType getNativeType(int index) {
    return hiveTypes[index].getNativeType();
  }

  /**
   * Set boolean value for column
   *
   * @param index column index
   * @param value data
   */
  public void setBoolean(int index, boolean value) {
    booleans[index] = value;
  }

  /**
   * Set long value for column
   *
   * @param index column index
   * @param value data
   */
  public void setLong(int index, long value) {
    longs[index] = value;
  }

  /**
   * Set double value for column
   *
   * @param index column index
   * @param value data
   */
  public void setDouble(int index, double value) {
    doubles[index] = value;
  }

  /**
   * Set String value for column
   *
   * @param index column index
   * @param value data
   */
  public void setString(int index, String value) {
    strings[index] = value;
  }

  /**
   * Set Object value for column
   *
   * @param index column index
   * @param value data
   */
  public void setObject(int index, Object value) {
    objects[index] = value;
  }

  /**
   * Set null to column
   *
   * @param index column index
   * @param value data
   */
  public void setNull(int index, boolean value) {
    nulls[index] = value;
  }

  @Override
  public Object get(int index) {
    if (nulls[index]) {
      return null;
    }
    if (hiveTypes[index].isCollection()) {
      return objects[index];
    } else {
      return getPrimitive(index);
    }
  }

  @Override
  public Object get(int index, HiveType hiveType) {
    return Records.get(this, index, hiveType);
  }

  /**
   * Get primitive column value
   *
   * @param index column index
   * @return Object
   */
  private Object getPrimitive(int index) {
    NativeType nativeType = hiveTypes[index].getNativeType();
    switch (nativeType) {
      case BOOLEAN:
        return booleans[index];
      case LONG:
        return longs[index];
      case DOUBLE:
        return doubles[index];
      case STRING:
        return strings[index];
      default:
        throw new IllegalArgumentException("Don't know how to handle native type " + nativeType);
    }
  }

  @Override
  public boolean getBoolean(int index) {
    Records.verifyType(index, HiveType.BOOLEAN, hiveTypes[index]);
    return booleans[index];
  }

  @Override
  public byte getByte(int index) {
    Records.verifyType(index, HiveType.BYTE, hiveTypes[index]);
    return (byte) longs[index];
  }

  @Override
  public short getShort(int index) {
    Records.verifyType(index, HiveType.SHORT, hiveTypes[index]);
    return (short) longs[index];
  }

  @Override
  public int getInt(int index) {
    Records.verifyType(index, HiveType.INT, hiveTypes[index]);
    return (int) longs[index];
  }

  @Override
  public long getLong(int index) {
    Records.verifyType(index, HiveType.LONG, hiveTypes[index]);
    return longs[index];
  }

  @Override
  public float getFloat(int index) {
    Records.verifyType(index, HiveType.FLOAT, hiveTypes[index]);
    return (float) doubles[index];
  }

  @Override
  public double getDouble(int index) {
    Records.verifyType(index, HiveType.DOUBLE, hiveTypes[index]);
    return doubles[index];
  }

  @Override
  public String getString(int index) {
    Records.verifyType(index, HiveType.STRING, hiveTypes[index]);
    return strings[index];
  }

  @Override
  public <K, V> Map<K, V> getMap(int index) {
    Records.verifyMapLike(index, hiveTypes[index]);
    return (Map<K, V>) objects[index];
  }

  @Override
  public <T> List<T> getList(int index) {
    Records.verifyType(index, HiveType.LIST, hiveTypes[index]);
    return (List<T>) objects[index];
  }

  @Override
  public boolean isNull(int index) {
    return nulls[index];
  }

  @Override
  public String toString() {
    Objects.ToStringHelper tsh = Objects.toStringHelper(this);
    for (int i = 0; i < getSize(); ++i) {
      tsh.add("type[" + i + "]", hiveTypes[i]);
      tsh.add("data[" + i + "]", get(i));
    }
    return tsh.toString();
  }
}
