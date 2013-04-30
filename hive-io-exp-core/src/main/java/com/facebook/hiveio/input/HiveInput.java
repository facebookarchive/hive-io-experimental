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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.RecordReaderWrapper;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.google.common.collect.AbstractIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class HiveInput {
  private static final Logger LOG = LoggerFactory.getLogger(HiveInput.class);

  public static Iterable<HiveReadableRecord> readTable(HiveInputDescription inputDesc)
      throws IOException, InterruptedException
  {
    String profileID = Long.toString(System.currentTimeMillis());

    final Configuration conf = new HiveConf(HiveInput.class);
    HiveApiInputFormat.setProfileInputDesc(conf, inputDesc, profileID);

    final HiveApiInputFormat inputFormat = new HiveApiInputFormat();
    inputFormat.setMyProfileId(profileID);

    JobContext jobContext = new JobContext(conf, new JobID());
    final List<InputSplit> splits = inputFormat.getSplits(jobContext);

    return new Iterable<HiveReadableRecord>() {
      @Override public Iterator<HiveReadableRecord> iterator() {
        return new RecordIterator(inputFormat, conf, splits.iterator());
      }
    };
  }

  private static class RecordIterator extends AbstractIterator<HiveReadableRecord> {
    private final HiveApiInputFormat inputFormat;
    private final Iterator<InputSplit> splits;
    private final TaskAttemptContext taskContext;
    private RecordReaderWrapper<HiveReadableRecord> recordReader;

    public RecordIterator(HiveApiInputFormat inputFormat, Configuration conf,
        Iterator<InputSplit> splits) {
      this.inputFormat = inputFormat;
      this.splits = splits;
      this.taskContext = new TaskAttemptContext(conf, new TaskAttemptID());
    }

    @Override
    protected HiveReadableRecord computeNext() {
      while (recordReader == null || !recordReader.hasNext()) {
        RecordReader<WritableComparable, HiveReadableRecord> reader = null;
        while (splits.hasNext() && reader == null) {
          InputSplit split = splits.next();
          try {
            reader = inputFormat.createRecordReader(split, taskContext);
            reader.initialize(split, taskContext);
          } catch (Exception e) {
            LOG.info("Couldn't create reader from split {}", split, e);
          }
        }
        if (reader == null) {
          return endOfData();
        } else {
          recordReader = new RecordReaderWrapper<HiveReadableRecord>(reader);
        }
      }

      return recordReader.next();
    }
  }
}
