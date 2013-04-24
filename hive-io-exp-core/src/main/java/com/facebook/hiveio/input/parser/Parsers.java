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
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.HiveTableName;
import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.conf.ClassConfOption;
import com.facebook.hiveio.input.parser.array.ArrayParser;
import com.facebook.hiveio.input.parser.array.ArrayParserData;
import com.facebook.hiveio.input.parser.array.BytesParser;
import com.facebook.hiveio.input.parser.hive.DefaultParser;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class Parsers {
  private static final Logger LOG = LoggerFactory.getLogger(Parsers.class);

  public static final ClassConfOption<RecordParser> FORCE_PARSER =
      ClassConfOption.create("hiveio.input.parser", null, RecordParser.class);

  public static final Set<Class<? extends RecordParser>> CLASSES =
      ImmutableSet.<Class<? extends RecordParser>>builder()
        .add(BytesParser.class)
        .add(ArrayParser.class)
        .add(DefaultParser.class)
        .build();

  public static RecordParser<Writable> bestParser(Deserializer deserializer,
      int numColumns, int[] columnIndexes, HiveTableName tableName,
      String[] partitionValues, Writable exampleValue, Configuration conf)
  {
    ArrayParserData data = new ArrayParserData(deserializer, columnIndexes, numColumns,
        partitionValues);

    for (int i = 0; i < numColumns; ++i) {
      data.structFields[i] = data.inspector.getAllStructFieldRefs().get(i);
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
      parser = new BytesParser(partitionValues, numColumns, data);
    } else {
      parser = new ArrayParser(partitionValues, numColumns, data);
    }

    Class<? extends RecordParser> forcedParserClass = FORCE_PARSER.get(conf);
    if (forcedParserClass == null) {
      LOG.info("Using {} to parse hive records from table {}",
          parser.getClass().getSimpleName(), tableName.dotString());
    } else {
      LOG.info("Using {} chosen by user instead of {} to parse hive records from table {}",
          forcedParserClass.getSimpleName(), parser.getClass().getSimpleName(),
          tableName.dotString());
      parser = createForcedParser(deserializer, numColumns, partitionValues, data, forcedParserClass);
    }

    return parser;
  }

  private static RecordParser<Writable> createForcedParser(Deserializer deserializer,
      int numColumns, String[] partitionValues, ArrayParserData data,
      Class<? extends RecordParser> forcedParserClass)
  {
    RecordParser<Writable> forcedParser;
    if (BytesParser.class.equals(forcedParserClass)) {
      forcedParser = new BytesParser(partitionValues, numColumns, data);
    } else if (ArrayParser.class.equals(forcedParserClass)) {
      forcedParser = new ArrayParser(partitionValues, numColumns, data);
    } else if (DefaultParser.class.equals(forcedParserClass)) {
      forcedParser = new DefaultParser(deserializer, partitionValues, numColumns);
    } else {
      throw new IllegalArgumentException("Don't know how to create parser " +
          forcedParserClass.getCanonicalName());
    }
    return forcedParser;
  }
}
