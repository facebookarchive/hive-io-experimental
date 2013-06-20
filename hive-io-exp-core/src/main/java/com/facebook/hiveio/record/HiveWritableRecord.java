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

package com.facebook.hiveio.record;

import com.facebook.hiveio.common.HiveType;

import java.util.List;
import java.util.Map;

/**
 * Hive row for writing
 */
public interface HiveWritableRecord {
  /**
   * Set value for column at index.
   *
   * @param index Column offset
   * @param value Data for column
   * @deprecated
   *  use {@link HiveWritableRecord#set(int, Object, com.facebook.hiveio.common.HiveType)}
   *  or one of the setX() methods
   */
  @Deprecated
  void set(int index, Object value);

  /**
   * Set value with type for column at index.
   *
   * @param index column offset
   * @param value data for column
   * @param hiveType expected hive type
   */
  void set(int index, Object value, HiveType hiveType);

  /**
   * Set boolean value for column at index.
   *
   * @param index Column offset
   * @param value Data for column
   */
  void setBoolean(int index, boolean value);

  /**
   * Set byte value for column at index.
   *
   * @param index Column offset
   * @param value Data for column
   */
  void setByte(int index, byte value);

  /**
   * Set short value for column at index.
   *
   * @param index Column offset
   * @param value Data for column
   */
  void setShort(int index, short value);

  /**
   * Set int value for column at index.
   *
   * @param index Column offset
   * @param value Data for column
   */
  void setInt(int index, int value);

  /**
   * Set long value for column at index.
   *
   * @param index Column offset
   * @param value Data for column
   */
  void setLong(int index, long value);

  /**
   * Set float value for column at index.
   *
   * @param index Column offset
   * @param value Data for column
   */
  void setFloat(int index, float value);

  /**
   * Set double value for column at index.
   *
   * @param index Column offset
   * @param value Data for column
   */
  void setDouble(int index, double value);

  /**
   * Set double value for column at index.
   *
   * @param index Column offset
   * @param value Data for column
   */
  void setString(int index, String value);

  /**
   * Set Map value for column at index.
   *
   * @param index Column offset
   * @param data Data for column
   */
  void setMap(int index, Map data);

  /**
   * Set List value for column at index.
   *
   * @param index Column offset
   * @param data Data for column
   */
  void setList(int index, List data);

  /**
   * Get all columns.
   * Note this is needed for saving records, it is not here by accident.
   *
   * @return All the row's data
   */
  List<Object> getAllColumns();
}
