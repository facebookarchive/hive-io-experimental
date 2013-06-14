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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.HackJobContext;
import org.apache.hadoop.mapred.HackTaskAttemptContext;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.thrift.TException;

import com.facebook.hiveio.record.HiveWritableRecord;

import java.io.IOException;

/**
 * Simple API for writing to Hive
 */
public class HiveOutput {
  /** Don't construct */
  private HiveOutput() { }

  /**
   * Write records to a Hive table
   *
   * @param outputDesc description of Hive table
   * @param records the records to write
   * @throws TException
   * @throws IOException
   * @throws InterruptedException
   */
  public static void writeTable(HiveOutputDescription outputDesc,
    Iterable<HiveWritableRecord> records)
    throws TException, IOException, InterruptedException
  {
    long uniqueId = System.nanoTime();
    String taskAttemptIdStr = "attempt_200707121733_" + (int) uniqueId + "_m_000005_0";

    String profile = Long.toString(uniqueId);

    HiveConf conf = new HiveConf(HiveOutput.class);
    conf.setInt("mapred.task.partition", 1);
    conf.set("mapred.task.id", taskAttemptIdStr);

    HiveApiOutputFormat.initProfile(conf, outputDesc, profile);

    HiveApiOutputFormat outputFormat = new HiveApiOutputFormat();
    outputFormat.setMyProfileId(profile);

    TaskAttemptID taskAttemptID = TaskAttemptID.forName(taskAttemptIdStr);
    TaskAttemptContext taskContext = new HackTaskAttemptContext(new JobConf(conf), taskAttemptID);
    JobContext jobContext = new HackJobContext(new JobConf(conf), taskAttemptID.getJobID());

    RecordWriterImpl recordWriter = outputFormat.getRecordWriter(taskContext);

    HiveApiOutputCommitter committer = outputFormat.getOutputCommitter(taskContext);
    committer.setupJob(jobContext);

    committer.setupTask(taskContext);
    for (HiveWritableRecord record : records) {
      recordWriter.write(NullWritable.get(), record);
    }
    recordWriter.close(taskContext);
    committer.commitTask(taskContext);

    committer.commitJob(jobContext);
  }
}
