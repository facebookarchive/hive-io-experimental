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
package com.facebook.hiveio.mapreduce.output;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * InputSplit reader
 */
public class SplitReader extends RecordReader<NullWritable, MapWritable> {
  /** Iterator over input */
  private final Iterator<MapWritable> iter;
  /** Current data */
  private MapWritable currentValue;

  /**
   * Constructor
   *
   * @param rows map records
   */
  public SplitReader(List<MapWritable> rows) {
    iter = rows.iterator();
  }

  @Override public void close() throws IOException { }

  @Override public void initialize(InputSplit split, TaskAttemptContext context)
    throws IOException, InterruptedException { }

  @Override public boolean nextKeyValue() throws IOException, InterruptedException {
    if (iter.hasNext()) {
      currentValue = iter.next();
      return true;
    }
    return false;
  }

  @Override public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override public MapWritable getCurrentValue() throws IOException, InterruptedException {
    return currentValue;
  }

  @Override public float getProgress() throws IOException, InterruptedException {
    return 0;
  }
}
