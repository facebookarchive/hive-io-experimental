package com.facebook.giraph.hive.input.parser.array;

import com.facebook.giraph.hive.common.HiveType;
import com.facebook.giraph.hive.common.NativeType;
import com.facebook.giraph.hive.record.HiveReadableRecord;
import com.google.common.base.Objects;

import java.util.Arrays;

class ArrayRecord implements HiveReadableRecord {
  private final int numColumns;

  // Note that partition data and column data are stored together, with column
  // data coming before partition values.
  private final HiveType[] hiveTypes;
  private final boolean[] booleans;
  private final long[] longs;
  private final double[] doubles;
  private final String[] strings;
  private final Object[] objects;
  private final boolean[] nulls;

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

  public int getNumColumns() {
    return numColumns;
  }

  public int getNumPartitionValues() {
    return booleans.length - numColumns;
  }

  public int getSize() {
    return booleans.length;
  }

  public void reset() {
    Arrays.fill(nulls, false);
  }

  public HiveType getHiveType(int index) {
    return hiveTypes[index];
  }

  public NativeType getNativeType(int index) {
    return hiveTypes[index].getNativeType();
  }

  public void setBoolean(int index, boolean value) {
    booleans[index] = value;
  }

  public void setLong(int index, long value) {
    longs[index] = value;
  }

  public void setDouble(int index, double value) {
    doubles[index] = value;
  }

  public void setString(int index, String value) {
    strings[index] = value;
  }

  public void setObject(int index, Object value) {
    objects[index] = value;
  }

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

  private Object getPrimitive(int index) {
    switch (hiveTypes[index].getNativeType()) {
      case BOOLEAN:
        return booleans[index];
      case LONG:
        return longs[index];
      case DOUBLE:
        return doubles[index];
      case STRING:
        return strings[index];
    }
    return null;
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
    for (int i = 0; i < booleans.length; ++i) {
      tsh.add("row[" + i + "]", get(i));
    }
    return tsh.toString();
  }
}
