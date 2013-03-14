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

package com.facebook.giraph.hive.record;

import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.google.common.base.Objects;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Single record from a Hive table. Used for both reading and writing.
 */
public class HiveApiRecord implements HiveRecord {
  /** Logger */
  public static final Logger LOG = Logger.getLogger(HiveApiRecord.class);

  /** Raw data for row */
  private final Object[] rowData;

  /**
   * Constructor
   *
   * @param numColumns number of columns
   */
  public HiveApiRecord(int numColumns) {
    rowData = new Object[numColumns];
  }

  @Override
  public Object get(int index) {
    return rowData[index];
  }

  @Override
  public List<Object> getAll() {
    return Arrays.asList(rowData);
  }

  @Override
  public void set(int index, Object value) {
    rowData[index] = value;
  }

  public int getNumColumns() {
    return rowData.length;
  }

  /**
   * Parse a row
   *
   * @param value Row from Hive
   * @param deserializer Deserializer
   * @throws IOException I/O errors
   */
  public void parse(Writable value, Deserializer deserializer)
    throws IOException {

  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("numColumns", getNumColumns())
        .add("rowData", rowDataToString())
        .toString();
  }

  /**
   * String representation of row data for debugging
   * @return String dump of row data
   */
  private String rowDataToString() {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    for (int i = 0; i < rowData.length; ++i) {
      if (i > 0) {
        sb.append(',');
      }
      if (rowData[i] == null) {
        sb.append("(null)");
      } else {
        sb.append(rowData[i].getClass().getSimpleName());
        sb.append(":");
        sb.append(rowData[i].toString());
      }
    }
    sb.append(']');
    return sb.toString();
  }
}
