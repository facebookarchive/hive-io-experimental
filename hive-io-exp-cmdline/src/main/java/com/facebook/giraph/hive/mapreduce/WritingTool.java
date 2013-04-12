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
package com.facebook.giraph.hive.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.facebook.giraph.hive.common.HadoopUtils;
import com.facebook.giraph.hive.record.HiveWritableRecord;

/*
  CREATE TABLE hive_io_test (
    i1 BIGINT,
    i2 BIGINT,
    i3 BIGINT
  )
  PARTITIONED BY (ds STRING)
  TBLPROPERTIES ('RETENTION_PLATINUM'='90')
 */
public class WritingTool extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(WritingTool.class);

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    // TODO: make configurable through cmdline
    HadoopUtils.setPool(conf, "di.nonsla");
    HadoopUtils.setMapAttempts(conf, 1);

    Job job = new Job(conf, "hive-io-writing");
    if (job.getJar() == null) {
      job.setJarByClass(getClass());
    }
    job.setMapperClass(SampleMapper.class);
    job.setInputFormatClass(SampleInputFormat.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(HiveWritableRecord.class);
    job.setOutputFormatClass(SampleOutputFormat.class);

    job.setNumReduceTasks(0);

    job.submit();
    return job.waitForCompletion(true) ? 0 : 1;
  }
}
