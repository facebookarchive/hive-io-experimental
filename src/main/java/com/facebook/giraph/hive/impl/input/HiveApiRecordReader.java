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

package com.facebook.giraph.hive.impl.input;

import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.facebook.giraph.hive.HiveRecord;
import com.facebook.giraph.hive.impl.HiveApiRecord;
import com.facebook.giraph.hive.input.HiveApiInputObserver;

import java.io.IOException;
import java.util.List;

/**
 * RecordReader for Hive data
 */
public class HiveApiRecordReader
    extends RecordReader<WritableComparable, HiveRecord> {
  // CHECKSTYLE: stop LineLength
  /** Base record reader */
  private final org.apache.hadoop.mapred.RecordReader<WritableComparable, Writable> baseRecordReader;
  // CHECKSTYLE: resume LineLength
  /** Current key read */
  private final WritableComparable key;
  /** Current value read */
  private final Writable value;
  /** Hive Deserializer */
  private final Deserializer deserializer;
  /** Whether to reuse HiveRecord object */
  private final boolean reuseRecord;

  /** Holds current record */
  private HiveApiRecord record;

  /** Observer for operations here */
  private HiveApiInputObserver observer;

  // CHECKSTYLE: stop LineLength
  /**
   * Constructor
   *
   * @param baseRecordReader Base record reader
   * @param deserializer Deserializer
   * @param partitionValues partition info
   * @param numColumns total number of columns
   * @param reuseRecord whether to reuse HiveRecord objects
   */
  public HiveApiRecordReader(
      org.apache.hadoop.mapred.RecordReader<WritableComparable, Writable> baseRecordReader,
      Deserializer deserializer, List<String> partitionValues,
      int numColumns, boolean reuseRecord) {
    // CHECKSTYLE: resume LineLength
    this.baseRecordReader = baseRecordReader;
    this.key = baseRecordReader.createKey();
    this.value = baseRecordReader.createValue();
    this.deserializer = deserializer;
    this.reuseRecord = reuseRecord;
    this.record = new HiveApiRecord(numColumns, partitionValues);
    this.observer = NoOpInputObserver.get();
  }

  public HiveApiInputObserver getObserver() {
    return observer;
  }

  /**
   * Set observer to call back to
   *
   * @param observer Observer to callback to
   */
  public void setObserver(HiveApiInputObserver observer) {
    if (observer != null) {
      this.observer = observer;
    }
  }

  @Override
  public void initialize(InputSplit inputSplit,
                         TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException { }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!internalRead()) {
      return false;
    }
    parse();
    return true;
  }

  /**
   * Read a row
   *
   * @return true if read succeeded
   * @throws IOException I/O errors
   */
  private boolean internalRead() throws IOException {
    observer.beginReadRow();
    boolean result = baseRecordReader.next(key, value);
    observer.endReadRow(key, value);

    if (!result) {
      observer.hiveReadRowFailed();
      return false;
    }

    return true;
  }

  /**
   * Parse the row read
   *
   * @throws IOException I/O errors
   */
  private void parse() throws IOException {
    observer.beginParse();
    if (!reuseRecord) {
      record = new HiveApiRecord(record.getNumColumns(),
          record.getPartitionValues());
    }
    record.parse(value, deserializer);
    observer.endParse(record);
  }

  @Override
  public WritableComparable getCurrentKey()
    throws IOException, InterruptedException {
    return key;
  }

  @Override
  public HiveApiRecord getCurrentValue()
    throws IOException, InterruptedException {
    return record;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return baseRecordReader.getProgress();
  }

  @Override
  public void close() throws IOException {
    baseRecordReader.close();
  }
}
