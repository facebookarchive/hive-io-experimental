package com.facebook.giraph.hive.input.parser.array;

import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;

import com.facebook.giraph.hive.common.HiveType;
import com.facebook.giraph.hive.common.NativeType;
import com.facebook.giraph.hive.input.parser.RecordParser;
import com.facebook.giraph.hive.record.HiveReadableRecord;
import com.google.common.base.Charsets;

import java.io.IOException;
import java.util.List;

public class BytesParser implements RecordParser {
  private final ArrayRecord record;
  private final int[] columnIndexes;

  public BytesParser(Deserializer deserializer, String[] partitionValues,
                     int numColumns, List<Integer> readColumnIds) {
    record = new ArrayRecord(numColumns + partitionValues.length);
    columnIndexes = new int[numColumns];

    StructObjectInspector inspector;
    try {
      inspector = (StructObjectInspector) deserializer.getObjectInspector();
    } catch (SerDeException e) {
      throw new IllegalStateException(e);
    }

    HiveType[] hiveTypes = new HiveType[numColumns];

    initColumns(inspector, numColumns, readColumnIds, hiveTypes);
    initPartitionValues(numColumns, partitionValues);
  }

  private void initColumns(StructObjectInspector inspector, int numColumns,
                           List<Integer> readColumnIds, HiveType[] hiveTypes) {
    for (int i = 0; i < numColumns; ++i) {
      StructField structField = inspector.getAllStructFieldRefs().get(i);
      ObjectInspector fieldInspector = structField.getFieldObjectInspector();
      hiveTypes[i] = HiveType.fromHiveObjectInspector(fieldInspector);
      if (hiveTypes[i].isPrimitive()) {
        record.setType(i, hiveTypes[i].getNativeType());
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
  public HiveReadableRecord parse(Writable value, HiveReadableRecord record) throws IOException {
    BytesRefArrayWritable braw = (BytesRefArrayWritable) value;
    ArrayRecord arrayRecord = (ArrayRecord) record;

    arrayRecord.reset();

    for (int i = 0; i < columnIndexes.length; i++) {
      int column = columnIndexes[i];
      BytesRefWritable fieldData = braw.unCheckedGet(column);

      byte[] bytes = fieldData.getData();
      int start = fieldData.getStart();
      int length = fieldData.getLength();

      if (length == "\\N".length() && bytes[start] == '\\' &&
          bytes[start + 1] == 'N') {
        arrayRecord.setNull(column, true);
      } else {
        parsePrimitiveColumn(column, bytes, start, length);
      }
    }

    return arrayRecord;
  }

  private void parsePrimitiveColumn(int column, byte[] bytes, int start, int length) {
    switch (record.getType(column)) {
      case BOOLEAN:
        Boolean bool = ParseHelpers.parseBoolean(bytes, start, length);
        if (bool == null) {
          record.setNull(column, true);
        } else {
          record.setBoolean(column, bool);
        }
        break;
      case LONG:
        if (length == 0) {
          record.setNull(column, true);
        } else {
          record.setLong(column, ParseHelpers.parseLong(bytes, start, length));
        }
        break;
      case DOUBLE:
        if (length == 0) {
          record.setNull(column, true);
        } else {
          record.setDouble(column, ParseHelpers.parseDouble(bytes, start, length));
        }
        break;
      case STRING:
        record.setString(column, new String(bytes, start,
            start + length, Charsets.UTF_8));
        break;
      default:
        break;
    }
  }
}
