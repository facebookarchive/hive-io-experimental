package com.facebook.giraph.hive.input.parser;

import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;

import com.facebook.giraph.hive.record.HiveRecord;

import java.io.IOException;

public class BytesParser implements RecordParser {
  private final Deserializer deserializer;

  private final int[] columnIndexes;

  private final long[] longs;
  private final double[] doubles;
  private final String[] strings;
  private final boolean[] nulls;

  public BytesParser(Deserializer deserializer, int numColumns) {
    this.deserializer = deserializer;

    longs = new long[numColumns];
    doubles = new double[numColumns];
    strings = new String[numColumns];
    nulls = new boolean[numColumns];

    try {
      StructObjectInspector structInspector =
          (StructObjectInspector) deserializer.getObjectInspector();

      for (int i = 0; i < numColumns; ++i) {

      }

    } catch (SerDeException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public HiveRecord parse(Writable value, HiveRecord record) throws IOException {
    for (int i = 0; i < columnIndexes.length; ++i) {

    }
  }
}
