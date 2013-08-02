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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.HiveTableDesc;
import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.conf.ClassConfOption;
import com.facebook.hiveio.input.parser.array.ArrayParser;
import com.facebook.hiveio.input.parser.array.ArrayParserData;
import com.facebook.hiveio.input.parser.array.BytesParser;
import com.facebook.hiveio.input.parser.hive.DefaultParser;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Helpers for parsers
 */
public class Parsers {
  /** Force a particular parser to be used */
  public static final ClassConfOption<RecordParser> FORCE_PARSER =
      ClassConfOption.create("hiveio.input.parser", null, RecordParser.class);

  /** Known parsers */
  public static final Set<Class<? extends RecordParser>> CLASSES =
      ImmutableSet.<Class<? extends RecordParser>>builder()
        .add(BytesParser.class)
        .add(ArrayParser.class)
        .add(DefaultParser.class)
        .build();

  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(Parsers.class);

  /**
   * NullStructField
   */
  private static class NullStructField implements StructField {
    @Override
    public String getFieldName() {
      return null;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public String getFieldComment() {
      return "";
    }
  }

  /**
   * NullStructField
   */
  private static final NullStructField NULL_STRUCT_FIELD = new NullStructField();

  /** Don't construct */
  private Parsers() { }

  /**
   * Choose the best parser available
   *
   * @param deserializer Hive Deserializer
   * @param schema Hive table schema
   * @param columnIndexes column IDs
   * @param partitionValues partition data
   * @param exampleValue example Writable
   * @param conf Configuration
   * @return RecordParser
   */
  public static RecordParser<Writable> bestParser(Deserializer deserializer,
      HiveTableSchema schema, int[] columnIndexes, String[] partitionValues,
      Writable exampleValue, Configuration conf)
  {
    ArrayParserData data = new ArrayParserData(deserializer, columnIndexes, schema,
        partitionValues);

    int numColumns = schema.numColumns();
    HiveTableDesc tableDesc = schema.getTableDesc();

    for (int i = 0; i < numColumns; ++i) {
      data.structFields[i] = i < data.inspector.getAllStructFieldRefs().size() ?
          data.inspector.getAllStructFieldRefs().get(i) : NULL_STRUCT_FIELD;
      ObjectInspector fieldInspector = data.structFields[i].getFieldObjectInspector();
      data.hiveTypes[i] = HiveType.fromHiveObjectInspector(fieldInspector);
      if (data.hiveTypes[i].isPrimitive()) {
        data.primitiveInspectors[i] = (PrimitiveObjectInspector) fieldInspector;
      }
    }

    boolean hasCollections = false;

    for (int i = 0; i < columnIndexes.length; ++i) {
      int columnId = columnIndexes[i];
      if (data.hiveTypes[columnId].isCollection()) {
        hasCollections = true;
        break;
      }
    }

    RecordParser<Writable> parser = null;

    if (!hasCollections && exampleValue instanceof BytesRefArrayWritable) {
      parser = new BytesParser(partitionValues, data);
    } else {
      parser = new ArrayParser(partitionValues, data);
    }

    Class<? extends RecordParser> forcedParserClass = FORCE_PARSER.get(conf);
    if (forcedParserClass == null) {
      LOG.info("Using {} to parse hive records from table {}",
          parser.getClass().getSimpleName(), tableDesc.dotString());
    } else {
      LOG.info("Using {} chosen by user instead of {} to parse hive records from table {}",
          forcedParserClass.getSimpleName(), parser.getClass().getSimpleName(),
          tableDesc.dotString());
      parser = createForcedParser(deserializer, schema, partitionValues,
          data, forcedParserClass);
    }

    return parser;
  }

  /**
   * Create a parser forced by user
   *
   * @param deserializer Hive Deserializer
   * @param schema Hive table schema
   * @param partitionValues partition data
   * @param data array parser data
   * @param forcedParserClass class of record parser
   * @return RecordParser
   */
  private static RecordParser<Writable> createForcedParser(Deserializer deserializer,
      HiveTableSchema schema, String[] partitionValues, ArrayParserData data,
      Class<? extends RecordParser> forcedParserClass)
  {
    RecordParser<Writable> forcedParser;
    if (BytesParser.class.equals(forcedParserClass)) {
      forcedParser = new BytesParser(partitionValues, data);
    } else if (ArrayParser.class.equals(forcedParserClass)) {
      forcedParser = new ArrayParser(partitionValues, data);
    } else if (DefaultParser.class.equals(forcedParserClass)) {
      forcedParser = new DefaultParser(deserializer, partitionValues, schema);
    } else {
      throw new IllegalArgumentException("Don't know how to create parser " +
          forcedParserClass.getCanonicalName());
    }
    return forcedParser;
  }
}
