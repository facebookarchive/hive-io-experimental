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

import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.schema.HiveTableSchema;

/**
 * Parser data for array parsers
 */
public class ArrayParserData {
  // CHECKSTYLE: stop VisibilityModifier
  /** Hive deserializer */
  public final Deserializer deserializer;
  /** Hive object inspector */
  public final StructObjectInspector inspector;

  /** Column IDs */
  public final int[] columnIndexes;

  /** Hive primitive object inspectors */
  public final PrimitiveObjectInspector[] primitiveInspectors;
  /** Hive struct fields */
  public final StructField[] structFields;
  /** Types of columns */
  public final HiveType[] hiveTypes;
  /** Hive table schema */
  public final HiveTableSchema schema;
  // CHECKSTYLE: resume VisibilityModifier

  /**
   * Constructor
   *
   * @param deserializer Hive Deserialier
   * @param columnIndexes column IDs
   * @param schema Hive table schema
   * @param partitionValues partition data
   */
  public ArrayParserData(Deserializer deserializer, int[] columnIndexes,
                         HiveTableSchema schema, String[] partitionValues)
  {
    this.schema = schema;
    this.deserializer = deserializer;

    this.columnIndexes = columnIndexes;

    int numColumns = schema.numColumns();

    this.primitiveInspectors = new PrimitiveObjectInspector[numColumns];
    this.structFields = new StructField[numColumns];

    this.hiveTypes = new HiveType[numColumns + partitionValues.length];
    for (int partIndex = 0; partIndex < partitionValues.length; ++partIndex) {
      hiveTypes[partIndex + numColumns] = HiveType.STRING;
    }

    try {
      inspector = (StructObjectInspector) deserializer.getObjectInspector();
    } catch (SerDeException e) {
      throw new IllegalStateException(e);
    }
  }
}
