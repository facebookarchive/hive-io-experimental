package com.facebook.giraph.hive.input.parser.generic;

import com.facebook.giraph.hive.common.NativeType;
import com.facebook.giraph.hive.record.HiveReadableRecord;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkArgument;

class ArrayRecord implements HiveReadableRecord {
  // Note that partition data and column data are stored together, with column
  // data coming before partition values.

  private final boolean[] booleans;
  private final long[] longs;
  private final double[] doubles;
  private final String[] strings;
  private final boolean[] nulls;
  private final NativeType[] types;

  public ArrayRecord(boolean[] booleans, long[] longs, double[] doubles,
                     String[] strings, boolean[] nulls, NativeType[] types) {
    checkArgument(booleans.length == longs.length);
    checkArgument(booleans.length == doubles.length);
    checkArgument(booleans.length == strings.length);
    checkArgument(booleans.length == nulls.length);
    checkArgument(booleans.length == types.length);

    this.booleans = booleans;
    this.longs = longs;
    this.doubles = doubles;
    this.strings = strings;
    this.nulls = nulls;
    this.types = types;
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
    verifyType(index, NativeType.BOOLEAN);
    return booleans[index];
  }

  @Override
  public long getLong(int index) {
    verifyType(index, NativeType.LONG);
    return longs[index];
  }

  @Override
  public double getDouble(int index) {
    verifyType(index, NativeType.DOUBLE);
    return doubles[index];
  }

  @Override
  public String getString(int index) {
    verifyType(index, NativeType.STRING);
    return strings[index];
  }

  @Override
  public boolean isNull(int index) {
    return nulls[index];
  }

  private void verifyType(int index, NativeType expectedType) {
    // Commented out for efficiency.
//    validateType(index, expectedType);
  }

  private void validateType(int index, NativeType expectedType) {
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
