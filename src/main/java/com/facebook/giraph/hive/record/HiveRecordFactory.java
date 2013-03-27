package com.facebook.giraph.hive.record;

import com.facebook.giraph.hive.input.parser.hive.DefaultRecord;

public class HiveRecordFactory {
  private HiveRecordFactory() {}

  public static HiveWritableRecord newWritableRecord(int numColumns) {
    return new DefaultRecord(numColumns, new String[0]);
  }
}
