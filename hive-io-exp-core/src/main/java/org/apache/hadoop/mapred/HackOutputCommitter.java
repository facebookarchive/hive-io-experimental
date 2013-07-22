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
package org.apache.hadoop.mapred;

import org.apache.hadoop.mapreduce.JobStatus;

import java.io.IOException;

/**
 * This is a hack to be able to work with TaskAttemptContext and JobContext from mapreduce.
 * Wraps mapred.OutputCommitter in mapreduce.OutputCommitter.
 */
public class HackOutputCommitter extends org.apache.hadoop.mapreduce.OutputCommitter {
  /** Wrapped OutputCommitter */
  private final OutputCommitter baseCommitter;
  /** JobConf to use */
  private final JobConf jobConf;

  /**
   * Constructor
   *
   * @param baseCommitter OutputCommitter which we want to wrap
   * @param jobConf JobConf to use
   */
  public HackOutputCommitter(OutputCommitter baseCommitter, JobConf jobConf) {
    this.baseCommitter = baseCommitter;
    this.jobConf = jobConf;
  }

  @Override
  public void setupJob(org.apache.hadoop.mapreduce.JobContext context) throws IOException {
    baseCommitter.setupJob(hackJobContext(context));
  }

  @Override
  public void setupTask(org.apache.hadoop.mapreduce.TaskAttemptContext context) throws IOException {
    baseCommitter.setupTask(hackTaskAttemptContext(context));
  }

  @Override
  public boolean needsTaskCommit(org.apache.hadoop.mapreduce.TaskAttemptContext context)
    throws IOException {
    return baseCommitter.needsTaskCommit(hackTaskAttemptContext(context));
  }

  @Override
  public void commitTask(org.apache.hadoop.mapreduce.TaskAttemptContext context)
    throws IOException {
    baseCommitter.commitTask(hackTaskAttemptContext(context));
  }

  @Override
  public void abortTask(org.apache.hadoop.mapreduce.TaskAttemptContext context)
    throws IOException {
    baseCommitter.abortTask(hackTaskAttemptContext(context));
  }

  @Override
  public void commitJob(org.apache.hadoop.mapreduce.JobContext context) throws IOException {
    baseCommitter.commitJob(hackJobContext(context));
  }

  @Override
  public void abortJob(org.apache.hadoop.mapreduce.JobContext context,
      JobStatus.State state) throws IOException {
    baseCommitter.abortJob(hackJobContext(context), state);
  }

  @Override
  public void cleanupJob(org.apache.hadoop.mapreduce.JobContext context) throws IOException {
    baseCommitter.cleanupJob(hackJobContext(context));
  }

  /**
   * Create mapred.JobContext from mapreduce.JobContext
   *
   * @param jobContext mapreduce.JobContext
   * @return mapred.JobContext
   */
  private JobContext hackJobContext(
      org.apache.hadoop.mapreduce.JobContext jobContext) {
    return new HackJobContext(jobConf, jobContext.getJobID());
  }

  /**
   * Create mapred.TaskAttemptContext from mapreduce.TaskAttemptContext
   *
   * @param taskAttemptContext mapreduce.TaskAttemptContext
   * @return mapred.TaskAttemptContext
   */
  private TaskAttemptContext hackTaskAttemptContext(
      org.apache.hadoop.mapreduce.TaskAttemptContext taskAttemptContext) {
    return new HackTaskAttemptContext(jobConf,
        TaskAttemptID.downgrade(taskAttemptContext.getTaskAttemptID()));
  }
}
