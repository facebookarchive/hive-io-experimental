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
package com.facebook.hiveio.input.parser;

import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;

/**
 * Base type for recr
 */
public class Records {
  /** Don't construct */
  private Records() { }

  /**
   * Get column data
   *
   * @param record HiveReadableRecord
   * @param index column index
   * @param hiveType HiveType
   * @return Object for column
   */
  public static Object get(HiveReadableRecord record, int index, HiveType hiveType) {
    switch (hiveType) {
      case BOOLEAN:
        return record.getBoolean(index);
      case BYTE:
        return record.getByte(index);
      case SHORT:
        return record.getShort(index);
      case INT:
        return record.getInt(index);
      case LONG:
        return record.getLong(index);
      case FLOAT:
        return record.getFloat(index);
      case DOUBLE:
        return record.getDouble(index);
      case STRING:
        return record.getString(index);
      case MAP:
      case STRUCT:
        return record.getMap(index);
      case LIST:
        return record.getList(index);
      default:
        throw new IllegalArgumentException("Don't know how to handle HiveType " + hiveType);
    }
  }

  /**
   * Verify column's type is as expected
   *
   * @param index column index
   * @param expectedType HiveType expected
   * @param schema HiveTableSchema
   */
  public static void verifyType(int index, HiveType expectedType, HiveTableSchema schema) {
    verifyType(index, expectedType, schema.columnType(index));
  }

  /**
   * Verify column's type is as expected
   *
   * @param index column index
   * @param expectedType HiveType expected
   * @param actualType HiveType actual
   */
  public static void verifyType(int index, HiveType expectedType, HiveType actualType) {
    if (actualType != expectedType) {
      throw new IllegalArgumentException("Expected column " + index +
          " to be " + expectedType + ", but is actually " + actualType);
    }
  }

  /**
   * Check that column data is map-like
   *
   * @param index column index
   * @param schema HiveTableSchema
   */
  public static void verifyMapLike(int index, HiveTableSchema schema) {
    verifyMapLike(index, schema.columnType(index));
  }

  /**
   * Check that column data is map-like
   *
   * @param index column index
   * @param actualType HiveType
   */
  public static void verifyMapLike(int index, HiveType actualType) {
    if (!actualType.isMapLike()) {
      throw new IllegalArgumentException("Expected column " + index +
          " to be MAP or STRUCT, but is actually " + actualType);
    }
  }
}
