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

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.facebook.giraph.hive.output.HiveApiOutputFormat;
import com.facebook.giraph.hive.output.HiveOutputDescription;
import com.facebook.giraph.hive.record.HiveWritableRecord;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Map;

import static com.facebook.giraph.hive.mapreduce.SampleOutputFormat.SAMPLE_PROFILE_ID;

public class SampleMapper extends Mapper<NullWritable, MapWritable,
    NullWritable, HiveWritableRecord> {
  private static final Logger LOG = Logger.getLogger(SampleMapper.class);

  @Override protected void setup(Context context)
      throws IOException, InterruptedException {
    super.setup(context);
    HiveOutputDescription outputDesc = new HiveOutputDescription();
    outputDesc.setDbName("default");
    outputDesc.setTableName("hive_io_test");
    Map<String, String> partitionValues = ImmutableMap.of("ds", "2013-04-01");
    outputDesc.setPartitionValues(partitionValues);
    LOG.info("Writing to " + outputDesc);
    try {
      HiveApiOutputFormat.initProfile(context.getConfiguration(), outputDesc, SAMPLE_PROFILE_ID);
    } catch (TException e) {
      LOG.fatal("Failed to initialize profile " + outputDesc);
      throw new IOException(e);
    }
  }

  @Override protected void map(NullWritable key, MapWritable value,
      Context context) throws IOException, InterruptedException {
    context.write(key, HiveTools.mapToHiveRecord(value));
  }
}
