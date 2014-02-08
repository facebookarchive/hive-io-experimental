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

package com.facebook.hiveio.output;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.ProgressReporter;
import com.facebook.hiveio.record.HiveWritableRecord;

import java.io.IOException;
import java.util.List;

/**
 * RecordWriter for Hive
 */
public class RecordWriterImpl extends RecordWriter<WritableComparable, HiveWritableRecord> {
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(RecordWriterImpl.class);

  // CHECKSTYLE: stop LineLength
  /** Base Hadoop RecordWriter */
  protected org.apache.hadoop.mapred.RecordWriter<WritableComparable, Writable> baseWriter;
  // CHECKSTYLE: resume LineLength
  /** Hive record serializer */
  private final Serializer serializer;
  /** Hive table data inspector */
  private final ObjectInspector objectInspector;
  /** Observer */
  private HiveApiOutputObserver observer;

  // CHECKSTYLE: stop LineLength
  /**
   * Constructor
   *
   * @param baseWriter Hadoop RecordWriter
   * @param serializer Serializer
   * @param objectInspector ObjectInspector
   */
  public RecordWriterImpl(org.apache.hadoop.mapred.RecordWriter<WritableComparable, Writable> baseWriter, Serializer serializer, ObjectInspector objectInspector) {
    // CHECKSTYLE: resume LineLength
    this.baseWriter = baseWriter;
    this.serializer = serializer;
    this.objectInspector = objectInspector;
    this.observer = HiveApiOutputObserver.Empty.get();
  }

  public HiveApiOutputObserver getObserver() {
    return observer;
  }

  /**
   * Set observer
   *
   * @param observer Hive observer to set
   */
  public void setObserver(HiveApiOutputObserver observer) {
    if (observer != null) {
      this.observer = observer;
    }
  }

  @Override
  public void write(WritableComparable key, HiveWritableRecord value)
    throws IOException, InterruptedException {
    Writable serializedValue = serialize(key, value);
    write(key, value, serializedValue);
  }

  /**
   * Serialize a key/value
   *
   * @param key Key
   * @param value Value
   * @return Serialized value. Key is ignored.
   * @throws IOException I/O errors
   */
  private Writable serialize(WritableComparable key, HiveWritableRecord value)
    throws IOException
  {
    List<Object> rowData = value.getAllColumns();
    Writable serializedValue;
    observer.beginSerialize(key, value);
    try {
      serializedValue = serializer.serialize(rowData, objectInspector);
    } catch (SerDeException e) {
      observer.serializeFailed(key, value);
      throw new IOException("Serialize failed", e);
    }
    observer.endSerialize(key, value);
    return serializedValue;
  }

  /**
   * Write serialized data to Hive
   *
   * @param key Key
   * @param value Value
   * @param serializedValue Serialized value
   * @throws IOException I/O errors
   */
  private void write(WritableComparable key, HiveWritableRecord value,
                     Writable serializedValue) throws IOException {
    observer.beginWrite(key, value);
    baseWriter.write(NullWritable.get(), serializedValue);
    observer.endWrite(key, value);
  }

  @Override
  public void close(TaskAttemptContext context)
    throws IOException, InterruptedException {
    baseWriter.close(new ProgressReporter(context));
  }
}
