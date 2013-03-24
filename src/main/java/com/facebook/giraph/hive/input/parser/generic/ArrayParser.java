package com.facebook.giraph.hive.input.parser.generic;

import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;

import com.facebook.giraph.hive.common.HiveType;
import com.facebook.giraph.hive.common.NativeType;
import com.facebook.giraph.hive.input.parser.RecordParser;
import com.facebook.giraph.hive.record.HiveReadableRecord;

import java.io.IOException;
import java.util.List;

public class ArrayParser implements RecordParser {
  private final Deserializer deserializer;
  private final int[] columnIndexes;

  private final StructObjectInspector inspector;
  private final PrimitiveObjectInspector fieldInspectors[];
  private final StructField[] structFields;

  private final NativeType[] nativeTypes;

  private final boolean[] booleans;
  private final long[] longs;
  private final double[] doubles;
  private final String[] strings;
  private final boolean[] nulls;

  public ArrayParser(Deserializer deserializer, String[] partitionValues,
                     int numColumns, List<Integer> readColumnIds) {
    this.deserializer = deserializer;

    columnIndexes = new int[numColumns];

    booleans = new boolean[numColumns + partitionValues.length];
    longs = new long[numColumns + partitionValues.length];
    doubles = new double[numColumns + partitionValues.length];
    strings = new String[numColumns + partitionValues.length];
    nulls = new boolean[numColumns + partitionValues.length];
    nativeTypes = new NativeType[numColumns + partitionValues.length];

    try {
      inspector = (StructObjectInspector) deserializer.getObjectInspector();
    } catch (SerDeException e) {
      throw new IllegalStateException(e);
    }

    structFields = new StructField[numColumns];
    HiveType[] hiveTypes = new HiveType[numColumns];
    fieldInspectors = new PrimitiveObjectInspector[numColumns];

    initColumns(numColumns, readColumnIds, hiveTypes);
    initPartitionValues(numColumns, partitionValues);
  }

  private void initColumns(int numColumns, List<Integer> readColumnIds, HiveType[] hiveTypes) {
    for (int i = 0; i < numColumns; ++i) {
      structFields[i] = inspector.getAllStructFieldRefs().get(i);
      ObjectInspector fieldInspector = structFields[i].getFieldObjectInspector();
      hiveTypes[i] = HiveType.fromHiveObjectInspector(fieldInspector);
      if (hiveTypes[i].isPrimitive()) {
        nativeTypes[i] = hiveTypes[i].getNativeType();
        fieldInspectors[i] = (PrimitiveObjectInspector) fieldInspector;
      }
    }

    for (int i = 0; i < readColumnIds.size(); ++i) {
      int columnId = readColumnIds.get(i);
      if (!hiveTypes[columnId].isPrimitive()) {
        throw new IllegalArgumentException("Cannot read column " + columnId +
          "of non-primitive type " + hiveTypes[columnId]);
      }
      columnIndexes[i] = columnId;
    }
  }

  private void initPartitionValues(int numColumns, String[] partitionValues) {
    for (int i = 0; i < partitionValues.length; ++i) {
      strings[i + numColumns] = partitionValues[i];
      nativeTypes[i + numColumns] = NativeType.STRING;
    }
  }

  @Override
  public HiveReadableRecord createRecord() {
    return new ArrayRecord(booleans, longs, doubles, strings, nulls, nativeTypes);
  }

  @Override
  public HiveReadableRecord parse(Writable value, HiveReadableRecord record)
      throws IOException {
    ArrayRecord arrayRecord = (ArrayRecord) record;

    Object rowData;
    try {
      rowData = deserializer.deserialize(value);
    } catch (SerDeException e) {
      throw new IOException(e);
    }

    for (int i = 0; i < columnIndexes.length; ++i) {
      int columnIndex = columnIndexes[i];
      Object fieldData = inspector.getStructFieldData(rowData, structFields[columnIndex]);
      if (fieldData == nulls) {
        nulls[columnIndex] = true;
      }
      Object primitiveData = fieldInspectors[columnIndex].getPrimitiveJavaObject(fieldData);
      switch (nativeTypes[columnIndex]) {
        case BOOLEAN:
          booleans[columnIndex] = (Boolean) primitiveData;
          break;
        case LONG:
          longs[columnIndex] = ((Number) primitiveData).longValue();
          break;
        case DOUBLE:
          doubles[columnIndex] = ((Number) primitiveData).doubleValue();
          break;
        case STRING:
          strings[columnIndex] = (String) primitiveData;
          break;
      }
    }

    return arrayRecord;
  }
}
