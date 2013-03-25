package com.facebook.giraph.hive.input.parser.array;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.Writable;

import com.facebook.giraph.hive.input.parser.RecordParser;
import com.facebook.giraph.hive.record.HiveReadableRecord;

import java.io.IOException;

public class ArrayParser implements RecordParser {
  private final ArrayRecord record;
  private final ArrayParserData parserData;

  public ArrayParser(String[] partitionValues, int numColumns, ArrayParserData parserData) {
    this.parserData = parserData;

    record = new ArrayRecord(numColumns, partitionValues.length);

    record.initColumnTypes(parserData.hiveTypes);
    record.initPartitionValues(partitionValues);
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
      rowData = parserData.deserializer.deserialize(value);
    } catch (SerDeException e) {
      throw new IOException(e);
    }

    for (int i = 0; i < parserData.columnIndexes.length; ++i) {
      int columnIndex = parserData.columnIndexes[i];
      Object fieldData = parserData.inspector.getStructFieldData(rowData,
          parserData.structFields[columnIndex]);
      if (fieldData == null) {
        arrayRecord.setNull(columnIndex, true);
        continue;
      }
      PrimitiveObjectInspector fieldInspector = parserData.fieldInspectors[columnIndex];
      Object primitiveData = fieldInspector.getPrimitiveJavaObject(fieldData);
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
