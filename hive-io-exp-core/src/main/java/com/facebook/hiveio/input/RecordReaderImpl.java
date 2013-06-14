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

package com.facebook.hiveio.input;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.facebook.hiveio.input.parser.RecordParser;
import com.facebook.hiveio.record.HiveReadableRecord;

import java.io.IOException;

/**
 * RecordReader for Hive data
 */
public class RecordReaderImpl extends RecordReader<WritableComparable, HiveReadableRecord> {
  // CHECKSTYLE: stop LineLength
  /** Base record reader */
  private final org.apache.hadoop.mapred.RecordReader<WritableComparable, Writable> baseRecordReader;
  // CHECKSTYLE: resume LineLength
  /** Current key read */
  private final WritableComparable key;
  /** Current value read */
  private final Writable value;
  /** Parser to use */
  private final RecordParser<Writable> parser;

  /** Holds current record */
  private HiveReadableRecord record;

  /** Observer for operations here */
  private HiveApiInputObserver observer;

  /**
   * Constructor
   *
   * @param baseRecordReader Base record reader
   * @param parser RecordParser
   */
  public RecordReaderImpl(
      org.apache.hadoop.mapred.RecordReader<WritableComparable, Writable> baseRecordReader,
      RecordParser parser)
  {
    this.baseRecordReader = baseRecordReader;
    this.key = baseRecordReader.createKey();
    this.value = baseRecordReader.createValue();
    this.observer = HiveApiInputObserver.Empty.get();
    this.parser = parser;
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
    throws IOException, InterruptedException {
    record = parser.createRecord();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    observer.beginReadRow();
    boolean result = baseRecordReader.next(key, value);
    observer.endReadRow(key, value);

    if (!result) {
      observer.hiveReadRowFailed();
      return false;
    }

    observer.beginParse();
    record = parser.parse(value, record);
    observer.endParse(record);

    return true;
  }

  @Override
  public WritableComparable getCurrentKey()
    throws IOException, InterruptedException {
    return key;
  }

  @Override
  public HiveReadableRecord getCurrentValue()
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
