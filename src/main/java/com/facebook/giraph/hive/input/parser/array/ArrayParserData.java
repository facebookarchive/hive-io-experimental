package com.facebook.giraph.hive.input.parser.array;

import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import com.facebook.giraph.hive.common.HiveType;

public class ArrayParserData {
  public final Deserializer deserializer;
  public final StructObjectInspector inspector;

  public final int[] columnIndexes;

  public final PrimitiveObjectInspector[] primitiveInspectors;
  public final StructField[] structFields;
  public final HiveType[] hiveTypes;

  public ArrayParserData(Deserializer deserializer, int[] columnIndexes,
                         int numColumns, String[] partitionValues) {
    this.deserializer = deserializer;

    this.columnIndexes = columnIndexes;

    this.primitiveInspectors = new PrimitiveObjectInspector[numColumns];
    this.structFields = new StructField[numColumns];

    this.hiveTypes = new HiveType[numColumns + partitionValues.length];
    for (int partIndex = 0; partIndex < partitionValues.length; ++partIndex) {
      hiveTypes[partIndex + numColumns] = HiveType.STRING;
    }

    try {
      inspector = (StructObjectInspector) deserializer.getObjectInspector();
    } catch (SerDeException e) {
      throw new IllegalStateException(e);
    }
  }
}
