package com.facebook.giraph.hive.input.parser.array;

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

  private final ArrayRecord record;

  public ArrayParser(Deserializer deserializer, String[] partitionValues,
                     int numColumns, List<Integer> readColumnIds) {
    this.deserializer = deserializer;

    record = new ArrayRecord(numColumns + partitionValues.length);
    columnIndexes = new int[numColumns];

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
        record.setType(i, hiveTypes[i].getNativeType());
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
      record.setType(i + numColumns, NativeType.STRING);
      record.setString(i + numColumns, partitionValues[i]);
    }
  }

  @Override
  public HiveReadableRecord createRecord() {
    return record;
  }

  @Override
  public HiveReadableRecord parse(Writable value, HiveReadableRecord record)
      throws IOException {
    ArrayRecord arrayRecord = (ArrayRecord) record;
    arrayRecord.reset();

    Object rowData;
    try {
      rowData = deserializer.deserialize(value);
    } catch (SerDeException e) {
      throw new IOException(e);
    }

    for (int i = 0; i < columnIndexes.length; ++i) {
      int columnIndex = columnIndexes[i];
      Object fieldData = inspector.getStructFieldData(rowData, structFields[columnIndex]);
      if (fieldData == null) {
        arrayRecord.setNull(columnIndex, true);
        continue;
      }
      Object primitiveData = fieldInspectors[columnIndex].getPrimitiveJavaObject(fieldData);
      switch (arrayRecord.getType(columnIndex)) {
        case BOOLEAN:
          arrayRecord.setBoolean(columnIndex, (Boolean) primitiveData);
          break;
        case LONG:
          arrayRecord.setLong(columnIndex, ((Number) primitiveData).longValue());
          break;
        case DOUBLE:
          arrayRecord.setDouble(columnIndex, ((Number) primitiveData).doubleValue());
          break;
        case STRING:
          arrayRecord.setString(columnIndex, (String) primitiveData);
          break;
      }
    }

    return arrayRecord;
  }
}
