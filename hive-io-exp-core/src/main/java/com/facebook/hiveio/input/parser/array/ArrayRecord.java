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
import com.facebook.hiveio.input.parser.Records;
import com.facebook.hiveio.record.HiveRecord;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Record backed by arrays
 */
public class ArrayRecord implements HiveRecord {
  /** Number of columns */
  private final int numColumns;

  // Note that partition data and column data are stored together, with column
  // data coming before partition values.
  /** Hive types */
  private final HiveType[] hiveTypes;
  /** booleans */
  private final boolean[] booleans;
  /** bytes */
  private final byte[] bytes;
  /** shorts */
  private final short[] shorts;
  /** ints */
  private final int[] ints;
  /** longs */
  private final long[] longs;
  /** doubles */
  private final float[] floats;
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
    this.bytes = new byte[size];
    this.shorts = new short[size];
    this.ints = new int[size];
    this.longs = new long[size];
    this.floats = new float[size];
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

  @Override
  public HiveType columnType(int index) {
    return hiveTypes[index];
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

  @Override
  public void setBoolean(int index, boolean value) {
    booleans[index] = value;
  }

  @Override
  public void setLong(int index, long value) {
    longs[index] = value;
  }

  @Override
  public void setDouble(int index, double value) {
    doubles[index] = value;
  }

  @Override
  public void setString(int index, String value) {
    strings[index] = value;
  }

  @Override
  public void set(int index, Object value) {
    set(index, value, hiveTypes[index]);
  }

  @Override
  public void set(int index, Object value, HiveType hiveType) {
    Records.verifyType(index, hiveType, hiveTypes[index]);
  }

  @Override
  public void setByte(int index, byte value) {
    bytes[index] = value;
  }

  @Override
  public void setShort(int index, short value) {
    shorts[index] = value;
  }

  @Override
  public void setInt(int index, int value) {
    ints[index] = value;
  }

  @Override
  public void setFloat(int index, float value) {
    floats[index] = value;
  }

  @Override
  public void setMap(int index, Map data) {
    objects[index] = data;
  }

  @Override
  public void setList(int index, List data) {
    objects[index] = data;
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
  public List<Object> getAllColumns() {
    List result = Lists.newArrayListWithCapacity(numColumns);
    for (int i = 0; i < numColumns(); ++i) {
      result.add(get(i));
    }
    return result;
  }

  @Override
  public Object get(int index) {
    if (nulls[index]) {
      return null;
    }
    return get(index, hiveTypes[index]);
  }

  @Override
  public Object get(int index, HiveType hiveType) {
    return Records.get(this, index, hiveType);
  }

  @Override
  public boolean getBoolean(int index) {
    Records.verifyType(index, HiveType.BOOLEAN, hiveTypes[index]);
    return booleans[index];
  }

  @Override
  public byte getByte(int index) {
    Records.verifyType(index, HiveType.BYTE, hiveTypes[index]);
    return bytes[index];
  }

  @Override
  public short getShort(int index) {
    Records.verifyType(index, HiveType.SHORT, hiveTypes[index]);
    return shorts[index];
  }

  @Override
  public int getInt(int index) {
    Records.verifyType(index, HiveType.INT, hiveTypes[index]);
    return ints[index];
  }

  @Override
  public long getLong(int index) {
    Records.verifyType(index, HiveType.LONG, hiveTypes[index]);
    return longs[index];
  }

  @Override
  public float getFloat(int index) {
    Records.verifyType(index, HiveType.FLOAT, hiveTypes[index]);
    return floats[index];
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
