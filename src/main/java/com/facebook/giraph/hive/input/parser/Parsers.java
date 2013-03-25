package com.facebook.giraph.hive.input.parser;

import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.facebook.giraph.hive.common.HiveType;
import com.facebook.giraph.hive.input.parser.array.ArrayParser;
import com.facebook.giraph.hive.input.parser.array.ArrayParserData;
import com.facebook.giraph.hive.input.parser.array.BytesParser;
import com.facebook.giraph.hive.input.parser.hive.DefaultParser;

public class Parsers {
  private static final Logger LOG = Logger.getLogger(Parsers.class);

  public static RecordParser<Writable> bestParser(Deserializer deserializer,
      int numColumns, int[] columnIndexes, String[] partitionValues, Writable exampleValue)
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

    for (int i = 0; i < columnIndexes.length; ++i) {
      int columnId = columnIndexes[i];
      if (!data.hiveTypes[columnId].isPrimitive()) {
        LOG.info("Hive table has non-primitives, using DefaultParser with it");
        return new DefaultParser(deserializer, partitionValues, numColumns);
      }
    }

    if (exampleValue instanceof BytesRefArrayWritable) {
      LOG.info("Using BytesParser to parse hive records");
      return new BytesParser(partitionValues, numColumns, data);
    } else {
      LOG.info("Using ArrayParser to parse hive records");
      return new ArrayParser(partitionValues, numColumns, data);
    }
  }
}
