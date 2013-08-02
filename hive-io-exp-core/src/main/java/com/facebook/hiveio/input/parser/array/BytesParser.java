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

/**
 * Hand written parser that is more performant
 */
public class BytesParser implements RecordParser {
  /** column IDs */
  private final int[] columnIndexes;
  /** record to write to */
  private final ArrayRecord record;

  /**
   * Constructor
   *
   * @param partitionValues partition data
   * @param parserData parser data
   */
  public BytesParser(String[] partitionValues, ArrayParserData parserData) {
    columnIndexes = parserData.columnIndexes;
    record = new ArrayRecord(parserData.schema.numColumns(), partitionValues,
        parserData.hiveTypes);
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
      if (braw.size() <= column) {
        arrayRecord.setNull(column, true);
        continue;
      }
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

  /**
   * Parse a primitive column
   *
   * @param column index of column
   * @param bytes raw byte[] of data
   * @param start offset to start of data
   * @param length size of data
   */
  private void parsePrimitiveColumn(int column, byte[] bytes, int start, int length) {
    switch (record.getHiveType(column)) {
      case BOOLEAN:
        Boolean bool = parseBoolean(bytes, start, length);
        if (bool == null) {
          record.setNull(column, true);
        } else {
          record.setBoolean(column, bool);
        }
        break;
      case BYTE:
        if (length == 0) {
          record.setNull(column, true);
        } else {
          record.setByte(column, bytes[start]);
        }
        break;
      case SHORT:
        if (length == 0) {
          record.setNull(column, true);
        } else {
          record.setShort(column, parseShort(bytes, start, length));
        }
        break;
      case INT:
        if (length == 0) {
          record.setNull(column, true);
        } else {
          record.setInt(column, parseInt(bytes, start, length));
        }
        break;
      case LONG:
        if (length == 0) {
          record.setNull(column, true);
        } else {
          record.setLong(column, parseLong(bytes, start, length));
        }
        break;
      case FLOAT:
        if (length == 0) {
          record.setNull(column, true);
        } else {
          record.setFloat(column, parseFloat(bytes, start, length));
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
        record.setString(column, parseString(bytes, start, length));
        break;
      default:
        throw new IllegalArgumentException("Unexpected HiveType " +
            record.getHiveType(column));
    }
  }

  /**
   * Parse a string from raw bytes
   *
   * @param bytes byte[]
   * @param start offset to start
   * @param length size of data
   * @return String
   */
  public static String parseString(byte[] bytes, int start, int length) {
    return new String(bytes, start, length, Charsets.UTF_8);
  }

  /**
   * Parse a boolean from raw bytes
   *
   * @param bytes byte[]
   * @param start offset to start
   * @param length size of data
   * @return Boolean
   */
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

  /**
   * Convert to upper case
   *
   * @param b byte to convert
   * @return upper case char
   */
  private static byte toUpperCase(byte b) {
    return isLowerCase(b) ? ((byte) (b - 32)) : b;
  }

  /**
   * Check if character is lower case
   *
   * @param b byte to check
   * @return true if char is lower case
   */
  private static boolean isLowerCase(byte b) {
    return (b >= 'a') && (b <= 'z');
  }

  /**
   * Parse short
   *
   * @param bytes byte[]
   * @param start offset to start
   * @param length size of data
   * @return short value
   */
  public static short parseShort(byte[] bytes, int start, int length) {
    return (short) parseLong(bytes, start, length);
  }


  /**
   * Parse int
   *
   * @param bytes byte[]
   * @param start offset to start
   * @param length size of data
   * @return int value
   */
  public static int parseInt(byte[] bytes, int start, int length) {
    return (int) parseLong(bytes, start, length);
  }

  /**
   * Parse long
   *
   * @param bytes byte[]
   * @param start offset to start
   * @param length size of data
   * @return long value
   */
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

  /**
   * Parse float
   *
   * @param bytes byte[]
   * @param start offset to start
   * @param length size of data
   * @return float value
   */
  public static float parseFloat(byte[] bytes, int start, int length) {
    char[] chars = new char[length];
    for (int pos = 0; pos < length; pos++) {
      chars[pos] = (char) bytes[start + pos];
    }
    String string = new String(chars);
    return Float.parseFloat(string);
  }

  /**
   * Parse double
   *
   * @param bytes byte[]
   * @param start offset to start
   * @param length size of data
   * @return double value
   */
  public static double parseDouble(byte[] bytes, int start, int length) {
    char[] chars = new char[length];
    for (int pos = 0; pos < length; pos++) {
      chars[pos] = (char) bytes[start + pos];
    }
    String string = new String(chars);
    return Double.parseDouble(string);
  }
}
