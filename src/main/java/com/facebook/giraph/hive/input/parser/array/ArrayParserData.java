package com.facebook.giraph.hive.input.parser.array;

import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import com.facebook.giraph.hive.common.HiveType;
import com.facebook.giraph.hive.common.NativeType;

public class ArrayParserData {
  public final Deserializer deserializer;
  public final StructObjectInspector inspector;

  public final int[] columnIndexes;

  public final PrimitiveObjectInspector[] fieldInspectors;
  public final StructField[] structFields;
  public final HiveType[] hiveTypes;
  public final NativeType[] nativeTypes;

  public ArrayParserData(Deserializer deserializer, int[] columnIndexes, int numColumns) {
    this.deserializer = deserializer;

    this.columnIndexes = columnIndexes;

    this.fieldInspectors = new PrimitiveObjectInspector[numColumns];
    this.structFields = new StructField[numColumns];
    this.hiveTypes = new HiveType[numColumns];
    this.nativeTypes = new NativeType[numColumns];

    try {
      inspector = (StructObjectInspector) deserializer.getObjectInspector();
    } catch (SerDeException e) {
      throw new IllegalStateException(e);
    }
  }
}
