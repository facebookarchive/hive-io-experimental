/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.hiveio.input.parser.array;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Writable;

import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.input.parser.RecordParser;
import com.facebook.hiveio.record.HiveReadableRecord;

import java.io.IOException;

/**
 * Parser using array of record data
 */
public class ArrayParser implements RecordParser {
  /** parser data */
  private final ArrayParserData parserData;
  /** record to store in */
  private final ArrayRecord record;

  /**
   * Constructor
   *
   * @param partitionValues partition info
   * @param parserData parser data
   */
  public ArrayParser(String[] partitionValues, ArrayParserData parserData) {
    this.parserData = parserData;
    this.record = new ArrayRecord(parserData.schema.numColumns(), partitionValues,
        parserData.hiveTypes);
  }

  @Override
  public HiveReadableRecord createRecord() {
    return record;
  }

  @Override
  public HiveReadableRecord parse(Writable value, HiveReadableRecord record)
    throws IOException
  {
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
      if (parserData.inspector.getAllStructFieldRefs().size() <= columnIndex) {
        arrayRecord.setNull(columnIndex, true);
        continue;
      }
      StructField structField = parserData.structFields[columnIndex];

      Object fieldData = parserData.inspector.getStructFieldData(rowData, structField);
      if (fieldData == null) {
        arrayRecord.setNull(columnIndex, true);
        continue;
      }

      if (arrayRecord.getHiveType(columnIndex).isCollection()) {
        ObjectInspector fieldInspector = structField.getFieldObjectInspector();
        Object parsed = ObjectInspectorUtils.copyToStandardJavaObject(fieldData, fieldInspector);
        arrayRecord.setObject(columnIndex, parsed);
      } else {
        parsePrimitive(arrayRecord, columnIndex, fieldData);
      }
    }

    return arrayRecord;
  }

  /**
   * Parse a primitive value
   *
   * @param arrayRecord record
   * @param columnIndex index of column
   * @param fieldData data to parse
   */
  private void parsePrimitive(ArrayRecord arrayRecord, int columnIndex, Object fieldData) {
    PrimitiveObjectInspector fieldInspector = parserData.primitiveInspectors[columnIndex];
    Object primitiveData = fieldInspector.getPrimitiveJavaObject(fieldData);

    HiveType hiveType = arrayRecord.getHiveType(columnIndex);
    switch (hiveType) {
      case BOOLEAN:
        arrayRecord.setBoolean(columnIndex, (Boolean) primitiveData);
        break;
      case BYTE:
        arrayRecord.setByte(columnIndex, (Byte) primitiveData);
        break;
      case SHORT:
        arrayRecord.setShort(columnIndex, (Short) primitiveData);
        break;
      case INT:
        arrayRecord.setInt(columnIndex, (Integer) primitiveData);
        break;
      case LONG:
        arrayRecord.setLong(columnIndex, (Long) primitiveData);
        break;
      case FLOAT:
        arrayRecord.setFloat(columnIndex, (Float) primitiveData);
        break;
      case DOUBLE:
        arrayRecord.setDouble(columnIndex, (Double) primitiveData);
        break;
      case STRING:
        arrayRecord.setString(columnIndex, (String) primitiveData);
        break;
      default:
        throw new IllegalArgumentException("Unexpected HiveType " + hiveType);
    }
  }
}
