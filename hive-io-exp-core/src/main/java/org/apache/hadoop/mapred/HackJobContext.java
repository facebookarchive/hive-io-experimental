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

import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.util.Progressable;

/**
 * This is a hack to expose mapred.JobContext which has package level constructors
 */
public class HackJobContext extends JobContext {
  /**
   * Constructor
   *
   * @param conf JobConf
   * @param jobId JobID
   */
  public HackJobContext(JobConf conf, org.apache.hadoop.mapreduce.JobID jobId) {
    super(conf, jobId);
  }

  /**
   * Constructor
   *
   * @param conf Configuration
   * @param jobId JobID
   * @param progress Progressable
   */
  public HackJobContext(JobConf conf, JobID jobId, Progressable progress) {
    super(conf, jobId, progress);
  }
}
