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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.facebook.giraph.hive.output.HiveApiOutputFormat;
import com.facebook.giraph.hive.output.HiveOutputDescription;
import com.facebook.giraph.hive.record.HiveRecordFactory;
import com.facebook.giraph.hive.record.HiveWritableRecord;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.facebook.giraph.hive.mapreduce.SampleOutputFormat.SAMPLE_PROFILE_ID;

public class HiveTools {
  private static final Logger LOG = Logger.getLogger(HiveTools.class);
  
  public static final int NUM_COLUMNS = 3;

  public static final MapWritable row1 = new MapWritable();
  public static final MapWritable row2 = new MapWritable();
  public static final MapWritable row3 = new MapWritable();
  public static final MapWritable row4 = new MapWritable();

  static {
    row1.put(new IntWritable(0), new LongWritable(23));
    row1.put(new IntWritable(1), new LongWritable(34));
    row1.put(new IntWritable(2), new LongWritable(45));

    row2.put(new IntWritable(0), new LongWritable(11));
    row2.put(new IntWritable(1), new LongWritable(22));
    row2.put(new IntWritable(2), new LongWritable(33));

    row3.put(new IntWritable(0), new LongWritable(67));
    row3.put(new IntWritable(1), new LongWritable(78));
    row3.put(new IntWritable(2), new LongWritable(89));

    row4.put(new IntWritable(0), new LongWritable(99));
    row4.put(new IntWritable(1), new LongWritable(88));
    row4.put(new IntWritable(2), new LongWritable(77));
  }

  public static final List<MapWritable> mapperData1 = ImmutableList.of(row1, row2);
  public static final List<MapWritable> mapperData2 = ImmutableList.of(row3, row4);

  private HiveTools() {}

  public static void setupJob(Configuration conf) throws IOException {
    HiveOutputDescription outputDesc = new HiveOutputDescription();
    outputDesc.setDbName("default");
    outputDesc.setTableName("hive_io_test");
    Map<String, String> partitionValues = ImmutableMap.of("ds", "2013-04-01");
    outputDesc.setPartitionValues(partitionValues);
    LOG.info("Writing to " + outputDesc);
    try {
      HiveApiOutputFormat.initProfile(conf, outputDesc, SAMPLE_PROFILE_ID);
    } catch (TException e) {
      LOG.fatal("Failed to initialize profile " + outputDesc);
      throw new IOException(e);
    }
  }

  public static HiveWritableRecord mapToHiveRecord(MapWritable value) {
    HiveWritableRecord record = HiveRecordFactory.newWritableRecord(HiveTools.NUM_COLUMNS);
    for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
      IntWritable intKey = (IntWritable) entry.getKey();
      LongWritable longValue = (LongWritable) entry.getValue();
      record.set(intKey.get(), longValue.get());
    }
    return record;
  }
}
