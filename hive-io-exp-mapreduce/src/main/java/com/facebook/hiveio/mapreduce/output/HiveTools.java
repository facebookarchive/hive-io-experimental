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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.HiveTableDesc;
import com.facebook.hiveio.output.HiveApiOutputFormat;
import com.facebook.hiveio.output.HiveOutputDescription;
import com.facebook.hiveio.record.HiveRecordFactory;
import com.facebook.hiveio.record.HiveWritableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.facebook.hiveio.schema.HiveTableSchemas;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Helpers for MapReduce job
 */
public class HiveTools {
  /** first row */
  private static final MapWritable ROW_1 = new MapWritable();
  /** second row */
  private static final MapWritable ROW_2 = new MapWritable();
  /** third row */
  private static final MapWritable ROW_3 = new MapWritable();
  /** fourth row */
  private static final MapWritable ROW_4 = new MapWritable();

  /** first mapper data */
  private static final List<MapWritable> MAPPER_DATA_1 = ImmutableList.of(ROW_1, ROW_2);
  /** second mapper data */
  private static final List<MapWritable> MAPPER_DATA_2 = ImmutableList.of(ROW_3, ROW_4);

  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(HiveTools.class);

  static {
    ROW_1.put(new IntWritable(0), new LongWritable(23));
    ROW_1.put(new IntWritable(1), new LongWritable(34));
    ROW_1.put(new IntWritable(2), new LongWritable(45));

    ROW_2.put(new IntWritable(0), new LongWritable(11));
    ROW_2.put(new IntWritable(1), new LongWritable(22));
    ROW_2.put(new IntWritable(2), new LongWritable(33));

    ROW_3.put(new IntWritable(0), new LongWritable(67));
    ROW_3.put(new IntWritable(1), new LongWritable(78));
    ROW_3.put(new IntWritable(2), new LongWritable(89));

    ROW_4.put(new IntWritable(0), new LongWritable(99));
    ROW_4.put(new IntWritable(1), new LongWritable(88));
    ROW_4.put(new IntWritable(2), new LongWritable(77));
  }

  /** Don't construct */
  private HiveTools() { }

  /**
   * Get first mapper data
   *
   * @return first mapper data
   */
  public static List<MapWritable> getMapperData1() {
    return MAPPER_DATA_1;
  }

  /**
   * Get second mapper data
   *
   * @return second mapper data
   */
  public static List<MapWritable> getMapperData2() {
    return MAPPER_DATA_2;
  }

  /**
   * Get name of table we're writing to
   *
   * @return HiveTableName
   */
  private static HiveTableDesc getHiveTableName() {
    return new HiveTableDesc("default", "hive_io_test");
  }

  /**
   * Setup the job
   *
   * @param conf Configuration
   * @throws IOException
   */
  public static void setupJob(Configuration conf) throws IOException {
    HiveOutputDescription outputDesc = new HiveOutputDescription();
    outputDesc.setTableDesc(getHiveTableName());
    Map<String, String> partitionValues = ImmutableMap.of("ds", "2013-04-01");
    outputDesc.setPartitionValues(partitionValues);
    LOG.info("Writing to {}", outputDesc);
    try {
      HiveApiOutputFormat.initProfile(conf, outputDesc, SampleOutputFormat.SAMPLE_PROFILE_ID);
    } catch (IOException e) {
      LOG.error("Failed to initialize profile {}", outputDesc);
      throw e;
    }
  }

  /**
   * Map hive record
   *
   * @param conf Configuration
   * @param value data
   * @return hive record
   */
  public static HiveWritableRecord mapToHiveRecord(Configuration conf, MapWritable value) {
    try {
      HiveTableSchema schema = HiveTableSchemas.lookup(conf, getHiveTableName());
      HiveWritableRecord record = HiveRecordFactory.newWritableRecord(schema);
      for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
        IntWritable intKey = (IntWritable) entry.getKey();
        LongWritable longValue = (LongWritable) entry.getValue();
        record.set(intKey.get(), longValue.get());
      }
      return record;
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
