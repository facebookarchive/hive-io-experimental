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
import com.facebook.hiveio.record.HiveReadableRecord;
import com.google.common.base.Objects;

import java.util.Arrays;

/**
 * Record backed by arrays
 */
class ArrayRecord implements HiveReadableRecord {
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
//    verifyType(index, NativeType.BOOLEAN);
    return booleans[index];
  }

  @Override
  public long getLong(int index) {
//    verifyType(index, NativeType.LONG);
    return longs[index];
  }

  @Override
  public double getDouble(int index) {
//    verifyType(index, NativeType.DOUBLE);
    return doubles[index];
  }

  @Override
  public String getString(int index) {
//    verifyType(index, NativeType.STRING);
    return strings[index];
  }

  @Override
  public boolean isNull(int index) {
    return nulls[index];
  }

  /**
   * Verify column's type is what we expect
   *
   * @param index column index
   * @param expectedType expected type
   */
  private void verifyType(int index, NativeType expectedType) {
    if (hiveTypes[index].getNativeType() != expectedType) {
      throw new IllegalStateException(
          String.format("Got an unexpected type %s from row %s for column %d, should be %s",
              hiveTypes[index], this, index, expectedType));
    }
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
