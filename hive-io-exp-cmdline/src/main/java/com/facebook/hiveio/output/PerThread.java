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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.HackJobContext;
import org.apache.hadoop.mapred.HackTaskAttemptContext;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.JobID;

class PerThread {
  public final Configuration conf;
  public final TaskAttemptID taskID;

  public PerThread(Configuration conf) {
    this.conf = new HiveConf(conf, OutputCmd.class);
    this.taskID = new TaskAttemptID("hiveio_output", 42, true,
        (int) Thread.currentThread().getId(), 0);
  }

  public TaskAttemptContext taskContext() {
    return new HackTaskAttemptContext(new JobConf(conf), taskID);
  }

  public JobID jobID() {
    return taskID.getJobID();
  }

  public JobConf jobConf() {
    return new JobConf(conf);
  }

  public JobContext jobContext() {
    return new HackJobContext(jobConf(), jobID());
  }
}
