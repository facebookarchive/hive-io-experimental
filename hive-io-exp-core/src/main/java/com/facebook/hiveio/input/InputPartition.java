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
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.Classes;
import com.facebook.hiveio.common.HadoopUtils;
import com.facebook.hiveio.common.Writables;
import com.google.common.base.Preconditions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Internal partition information for reading
 */
class InputPartition implements Writable {
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(InputPartition.class);

  /** Data for this partition */
  private final InputSplitData inputSplitData;
  /** Class for InputFormat to read this partition */
  private Class<? extends InputFormat> inputFormatClass;
  /** Location in Hadoop */
  private String location;

  /** Constructor for reflection */
  public InputPartition() {
    inputSplitData = new InputSplitData();
  }

  /**
   * Constructor with data
   * @param inputSplitData split data
   * @param inputFormatClass class for input format
   * @param location where data is on hdfs
   */
  public InputPartition(InputSplitData inputSplitData,
                        Class<? extends InputFormat> inputFormatClass,
                        String location) {
    this.inputSplitData = inputSplitData;
    this.inputFormatClass = inputFormatClass;
    this.location = location;
  }

  /**
   * Create for un-partitioned tables
   * @param table Hive table
   * @return InputPartition
   */
  public static InputPartition newFromHiveTable(Table table) {
    return makePartition(table.getSd(), Collections.<String>emptyList());
  }

  /**
   * Create for partition of a table
   * @param partition Hive partition
   * @return InputPartition
   */
  public static InputPartition newFromHivePartition(Partition partition) {
    return makePartition(partition.getSd(), partition.getValues());
  }

  /**
   * Helper to construct class
   * @param storageDescriptor {@link StorageDescriptor}
   * @param partitionValues list of partition values
   * @return InputPartition
   */
  private static InputPartition makePartition(StorageDescriptor storageDescriptor,
                                              List<String> partitionValues) {
    InputSplitData splitData = new InputSplitData(storageDescriptor, partitionValues);
    Class<? extends InputFormat> inputFormatClass = Classes
        .classForName(storageDescriptor.getInputFormat());
    String location = storageDescriptor.getLocation();
    return new InputPartition(splitData, inputFormatClass, location);
  }

  /**
   * Create InputFormat for this partition
   *
   * @param conf Configuration
   * @return InputFormat
   */
  public InputFormat makeInputFormat(Configuration conf) {
    InputFormat inputFormat = ReflectionUtils.newInstance(inputFormatClass, conf);
    HadoopUtils.configureInputFormat(inputFormat, conf);
    return inputFormat;
  }

  public InputSplitData getInputSplitData() {
    return inputSplitData;
  }

  public String getLocation() {
    return location;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Writables.writeClassName(out, Preconditions.checkNotNull(inputFormatClass));
    WritableUtils.writeString(out, Preconditions.checkNotNull(location));
    inputSplitData.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    inputFormatClass = Writables.readClass(in);
    location = WritableUtils.readString(in);
    inputSplitData.readFields(in);
  }
}
