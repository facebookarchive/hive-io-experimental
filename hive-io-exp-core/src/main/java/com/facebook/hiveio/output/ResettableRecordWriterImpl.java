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

import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.ProgressReporter;
import com.facebook.hiveio.record.HiveWritableRecord;

import java.io.IOException;

/**
 * RecordWriter implementation which tracks how long each single write is taking.
 * When it detects that write is too slow, it closes the previous file and starts writing to
 * the new one.
 */
public class ResettableRecordWriterImpl extends RecordWriterImpl {
  /** Class logger */
  private static final Logger LOG = LoggerFactory.getLogger(ResettableRecordWriterImpl.class);

  /** Task attempt context */
  private final TaskAttemptContext taskAttemptContext;
  /** Base output format */
  private final org.apache.hadoop.mapred.OutputFormat baseOutputFormat;
  /** When write takes longer than this time, new file will be opened to write to */
  private final long writeTimeoutMs;

  /**
   * Constructor
   *
   * @param baseWriter         Hadoop RecordWriter
   * @param serializer         Serializer
   * @param objectInspector    ObjectInspector
   * @param taskAttemptContext Task attempt context
   * @param baseOutputFormat   Base output format
   * @param writeTimeoutMs     When write takes longer than this time,
   *                           new file will be opened to write to
   */
  public ResettableRecordWriterImpl(RecordWriter<WritableComparable, Writable> baseWriter,
      Serializer serializer, ObjectInspector objectInspector, TaskAttemptContext taskAttemptContext,
      org.apache.hadoop.mapred.OutputFormat baseOutputFormat, long writeTimeoutMs) {
    super(baseWriter, serializer, objectInspector);
    this.taskAttemptContext = taskAttemptContext;
    this.baseOutputFormat = baseOutputFormat;
    this.writeTimeoutMs = writeTimeoutMs;
  }

  @Override
  public void write(WritableComparable key,
      HiveWritableRecord value) throws IOException, InterruptedException {
    long startTime = System.currentTimeMillis();
    super.write(key, value);
    long elapsedTime = System.currentTimeMillis() - startTime;
    if (elapsedTime > writeTimeoutMs) {
      LOG.info("write: Write taking too long ({}s), creating new file to write to", elapsedTime);
      baseWriter.close(new ProgressReporter(taskAttemptContext));
      baseWriter = HiveApiOutputFormat.getBaseRecordWriter(taskAttemptContext, baseOutputFormat);
    }
  }
}
