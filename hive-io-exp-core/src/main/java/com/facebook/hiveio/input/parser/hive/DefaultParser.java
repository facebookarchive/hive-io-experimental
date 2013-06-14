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
package com.facebook.hiveio.input.parser.hive;

import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;

import com.facebook.hiveio.input.parser.RecordParser;
import com.facebook.hiveio.record.HiveReadableRecord;

import java.io.IOException;
import java.util.List;

/**
 * Default parser using Hive inspectors
 */
public class DefaultParser implements RecordParser {
  /** Hive Deserializer */
  private final Deserializer deserializer;
  /** Row inspector */
  private final ObjectInspector rowInspector;
  /** Partition information */
  private final String[] partitionValues;
  /** Number of columns in the table */
  private final int numColumns;

  /**
   * Constructor
   *
   * @param deserializer Hive Deserializer
   * @param partitionValues partition data
   * @param numColumns number of columns
   */
  public DefaultParser(Deserializer deserializer, String[] partitionValues, int numColumns) {
    this.deserializer = deserializer;
    try {
      this.rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
    } catch (SerDeException e) {
      throw new IllegalArgumentException("Could not get object inspector", e);
    }

    this.partitionValues = partitionValues;
    this.numColumns = numColumns;
  }

  @Override
  public HiveReadableRecord createRecord() {
    return new DefaultRecord(numColumns, partitionValues);
  }

  @Override
  public HiveReadableRecord parse(Writable value, HiveReadableRecord record)
    throws IOException
  {
    DefaultRecord defaultRecord = (DefaultRecord) record;

    Object data;
    try {
      data = deserializer.deserialize(value);
    } catch (SerDeException e) {
      throw new IOException(e);
    }

    Object parsedData = ObjectInspectorUtils.copyToStandardJavaObject(data, rowInspector);
    List<Object> parsedList = (List<Object>) parsedData;
    for (int i = 0; i < parsedList.size(); ++i) {
      defaultRecord.set(i, parsedList.get(i));
    }

    return defaultRecord;
  }
}
