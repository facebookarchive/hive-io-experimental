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

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.Writable;

import com.facebook.hiveio.input.parser.RecordParser;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.google.common.base.Charsets;

import java.io.IOException;

public class BytesParser implements RecordParser {
  private final int[] columnIndexes;
  private final ArrayRecord record;

  public BytesParser(String[] partitionValues, int numColumns, ArrayParserData parserData) {
    columnIndexes = parserData.columnIndexes;
    record = new ArrayRecord(numColumns, partitionValues, parserData.hiveTypes);
  }

  @Override
  public HiveReadableRecord createRecord() {
    return record;
  }

  @Override
  public HiveReadableRecord parse(Writable value, HiveReadableRecord record) throws IOException {
    final BytesRefArrayWritable braw = (BytesRefArrayWritable) value;
    final ArrayRecord arrayRecord = (ArrayRecord) record;

    arrayRecord.reset();

    for (int i = 0; i < columnIndexes.length; i++) {
      final int column = columnIndexes[i];
      final BytesRefWritable fieldData = braw.unCheckedGet(column);

      final byte[] bytes = fieldData.getData();
      final int start = fieldData.getStart();
      final int length = fieldData.getLength();

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
    switch (record.getNativeType(column)) {
      case BOOLEAN:
        Boolean bool = parseBoolean(bytes, start, length);
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
          record.setLong(column, parseLong(bytes, start, length));
        }
        break;
      case DOUBLE:
        if (length == 0) {
          record.setNull(column, true);
        } else {
          record.setDouble(column, parseDouble(bytes, start, length));
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

  public static Boolean parseBoolean(byte[] bytes, int start, int length) {
    if ((length == 4) &&
        (toUpperCase(bytes[start + 0]) == 'T') &&
        (toUpperCase(bytes[start + 1]) == 'R') &&
        (toUpperCase(bytes[start + 2]) == 'U') &&
        (toUpperCase(bytes[start + 3]) == 'E')) {
      return true;
    }
    if ((length == 5) &&
        (toUpperCase(bytes[start + 0]) == 'F') &&
        (toUpperCase(bytes[start + 1]) == 'A') &&
        (toUpperCase(bytes[start + 2]) == 'L') &&
        (toUpperCase(bytes[start + 3]) == 'S') &&
        (toUpperCase(bytes[start + 4]) == 'E')) {
      return false;
    }
    return null;
  }

  private static byte toUpperCase(byte b) {
    return isLowerCase(b) ? ((byte) (b - 32)) : b;
  }

  private static boolean isLowerCase(byte b) {
    return (b >= 'a') && (b <= 'z');
  }

  public static long parseLong(byte[] bytes, int start, int length) {
    int limit = start + length;

    int sign = bytes[start] == '-' ? -1 : 1;

    if (sign == -1 || bytes[start] == '+') {
      start++;
    }

    long value = bytes[start++] - '0';
    while (start < limit) {
      value = value * 10 + (bytes[start] - '0');
      start++;
    }

    return value * sign;
  }

  public static double parseDouble(byte[] bytes, int start, int length) {
    char[] chars = new char[length];
    for (int pos = 0; pos < length; pos++) {
      chars[pos] = (char) bytes[start + pos];
    }
    String string = new String(chars);
    return Double.parseDouble(string);
  }
}
