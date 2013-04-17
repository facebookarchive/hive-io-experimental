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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.facebook.giraph.hive.record.HiveWritableRecord;

import java.io.IOException;

public class SampleMapper extends Mapper<NullWritable, MapWritable,
    NullWritable, HiveWritableRecord> {
  private static final Logger LOG = Logger.getLogger(SampleMapper.class);

  @Override protected void map(NullWritable key, MapWritable value,
      Context context) throws IOException, InterruptedException {
    context.write(key, HiveTools.mapToHiveRecord(value));
  }
}
