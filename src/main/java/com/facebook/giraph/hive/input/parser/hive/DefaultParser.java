package com.facebook.giraph.hive.input.parser.hive;

import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;

import com.facebook.giraph.hive.input.parser.RecordParser;
import com.facebook.giraph.hive.record.HiveReadableRecord;

import java.io.IOException;
import java.util.List;

public class DefaultParser implements RecordParser {
  /** Hive Deserializer */
  private final Deserializer deserializer;
  /** Row inspector */
  private final ObjectInspector rowInspector;
  /** Partition information */
  private final String[] partitionValues;
  /** Number of columns in the table */
  private final int numColumns;

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
      throws IOException {
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
