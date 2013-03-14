package com.facebook.giraph.hive.input.parser;

import org.apache.hadoop.io.Writable;

import com.facebook.giraph.hive.record.HiveRecord;

import java.io.IOException;

public class BytesParser implements RecordParser {
  @Override
  public HiveRecord parse(Writable value, HiveRecord record) throws IOException {
    return null;
  }
}
