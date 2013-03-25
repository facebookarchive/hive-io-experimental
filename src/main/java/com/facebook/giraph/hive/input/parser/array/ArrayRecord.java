package com.facebook.giraph.hive.input.parser.array;

import com.facebook.giraph.hive.common.NativeType;
import com.facebook.giraph.hive.record.HiveReadableRecord;
import com.google.common.base.Objects;

import java.util.Arrays;

class ArrayRecord implements HiveReadableRecord {
  // Note that partition data and column data are stored together, with column
  // data coming before partition values.

  private final NativeType[] types;
  private final boolean[] booleans;
  private final long[] longs;
  private final double[] doubles;
  private final String[] strings;
  private final boolean[] nulls;

  public ArrayRecord(int size) {
    this.types = new NativeType[size];
    this.booleans = new boolean[size];
    this.longs = new long[size];
    this.doubles = new double[size];
    this.strings = new String[size];
    this.nulls = new boolean[size];
  }

  public void reset() {
    Arrays.fill(nulls, false);
  }

  public NativeType getType(int index) {
    return types[index];
  }

  public void setType(int index, NativeType type) {
    types[index] = type;
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

  public void setNull(int index, boolean value) {
    nulls[index] = value;
  }

  @Override
  public Object get(int index) {
    if (types[index] == null) {
      return null;
    }
    switch (types[index]) {
      case BOOLEAN:
        return booleans[index];
      case LONG:
        return longs[index];
      case DOUBLE:
        return doubles[index];
      case STRING:
        return strings[index];
    }
    return nulls[index];
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
    if (types[index] != expectedType) {
      throw new IllegalStateException(
          String.format("Got an unexpected type %s from row %s for column %d, should be %s",
              types[index], this, index, expectedType));
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
