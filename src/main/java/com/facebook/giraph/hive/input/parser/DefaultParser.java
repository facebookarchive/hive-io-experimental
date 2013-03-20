package com.facebook.giraph.hive.input.parser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;

import com.facebook.giraph.hive.record.DefaultRecord;
import com.facebook.giraph.hive.record.HiveRecord;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DefaultParser implements RecordParser {
  /** Configuration key for whether to reuse records */
  public static final String REUSE_RECORD_KEY = "hive.api.input.reuse_record";

  /** Hive Deserializer */
  private final Deserializer deserializer;
  /** Row inspector */
  private final ObjectInspector rowInspector;
  /** Whether to reuse HiveRecord object */
  private final boolean reuseRecord;
  /** Partition information */
  private final Map<String, String> partitionValues;
  /** Number of columns in the table */
  private final int numColumns;

  public DefaultParser(Configuration conf, Deserializer deserializer,
                       Map<String, String> partitionValues, int numColumns) {
    this.deserializer = deserializer;
    try {
      this.rowInspector = deserializer.getObjectInspector();
    } catch (SerDeException e) {
      throw new IllegalStateException()
    }
    this.reuseRecord = conf.getBoolean(REUSE_RECORD_KEY, false);
    this.partitionValues = partitionValues;
    this.numColumns = numColumns;
  }

  @Override
  public HiveRecord parse(Writable value, HiveRecord record) throws IOException {
    if (!reuseRecord || record == null) {
      record = new DefaultRecord(numColumns, partitionValues);
    }

    Object data;;
    try {
      data = deserializer.deserialize(value);
      rowInspector = deserializer.getObjectInspector();
    } catch (SerDeException e) {
      throw new IOException(e);
    }

    Preconditions.checkArgument(
        rowInspector.getCategory() == ObjectInspector.Category.STRUCT);
    StructObjectInspector structInspector =
        (StructObjectInspector) rowInspector;

    Object parsedData = ObjectInspectorUtils.copyToStandardJavaObject(data, structInspector);
    List<Object> parsedList = (List<Object>) parsedData;
    for (int i = 0; i < parsedList.size(); ++i) {
      record.set(i, parsedList.get(i));
    }

    return record;
  }
}
