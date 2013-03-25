package com.facebook.giraph.hive.input.parser;

import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.facebook.giraph.hive.common.HiveTableName;
import com.facebook.giraph.hive.common.HiveType;
import com.facebook.giraph.hive.input.parser.array.ArrayParser;
import com.facebook.giraph.hive.input.parser.array.ArrayParserData;
import com.facebook.giraph.hive.input.parser.array.BytesParser;
import com.facebook.giraph.hive.input.parser.hive.DefaultParser;

public class Parsers {
  private static final Logger LOG = Logger.getLogger(Parsers.class);

  public static RecordParser<Writable> bestParser(Deserializer deserializer,
      int numColumns, int[] columnIndexes, HiveTableName tableName,
      String[] partitionValues, Writable exampleValue)
  {
    ArrayParserData data = new ArrayParserData(deserializer, columnIndexes, numColumns);

    for (int i = 0; i < numColumns; ++i) {
      data.structFields[i] = data.inspector.getAllStructFieldRefs().get(i);
      ObjectInspector fieldInspector = data.structFields[i].getFieldObjectInspector();
      data.hiveTypes[i] = HiveType.fromHiveObjectInspector(fieldInspector);
      if (data.hiveTypes[i].isPrimitive()) {
        data.nativeTypes[i] = data.hiveTypes[i].getNativeType();
        data.fieldInspectors[i] = (PrimitiveObjectInspector) fieldInspector;
      }
    }

    RecordParser<Writable> parser = null;

    for (int i = 0; i < columnIndexes.length; ++i) {
      int columnId = columnIndexes[i];
      if (!data.hiveTypes[columnId].isPrimitive()) {
        parser = new DefaultParser(deserializer, partitionValues, numColumns);
      }
    }

    if (parser == null) {
      if (exampleValue instanceof BytesRefArrayWritable) {
        parser = new BytesParser(partitionValues, numColumns, data);
      } else {
        parser = new ArrayParser(partitionValues, numColumns, data);
      }
    }

    LOG.info("Using " + parser.getClass().getSimpleName() +
        " to parse hive records from table " + tableName.dotString());
    return parser;
  }
}
