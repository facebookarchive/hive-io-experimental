package com.facebook.giraph.hive.input.parser;

import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;

import com.facebook.giraph.hive.record.HiveApiRecord;
import com.facebook.giraph.hive.record.HiveRecord;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.List;

public class DefaultParser implements RecordParser {
  /** Hive Deserializer */
  private final Deserializer deserializer;
  /** Whether to reuse HiveRecord object */
  private final boolean reuseRecord;
  private final int numColumns;

  public DefaultParser(Deserializer deserializer, boolean reuseRecord,
                       int numColumns) {
    this.deserializer = deserializer;
    this.reuseRecord = reuseRecord;
    this.numColumns = numColumns;
  }

  @Override
  public HiveRecord parse(Writable value, HiveRecord record) throws IOException {
    if (!reuseRecord || record == null) {
      record = new HiveApiRecord(numColumns);
    }

    Object data;
    ObjectInspector dataInspector;
    try {
      data = deserializer.deserialize(value);
      dataInspector = deserializer.getObjectInspector();
    } catch (SerDeException e) {
      throw new IOException(e);
    }

    Preconditions.checkArgument(
        dataInspector.getCategory() == ObjectInspector.Category.STRUCT);
    StructObjectInspector structInspector =
        (StructObjectInspector) dataInspector;

    Object parsedData = ObjectInspectorUtils.copyToStandardJavaObject(data, structInspector);
    List<Object> parsedList = (List<Object>) parsedData;
    for (int i = 0; i < parsedList.size(); ++i) {
      record.set(i, parsedList.get(i));
    }

    return record;
  }
}
