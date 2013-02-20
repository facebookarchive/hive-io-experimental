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

package org.apache.hadoop.hive.api.impl.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.api.impl.common.Classes;
import org.apache.hadoop.hive.api.impl.common.Writables;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Internal partition information for reading
 */
public class InputPartition implements Writable {
  /** Logger */
  private static final Logger LOG = Logger.getLogger(InputPartition.class);

  /** Data for this partition */
  private final InputSplitData inputSplitData;
  /** Class for InputFormat to read this partition */
  private Class<? extends InputFormat> inputFormatClass;
  /** Location in Hadoop */
  private String location;

  /**
   * Constructor
   */
  public InputPartition() {
    inputSplitData = new InputSplitData();
  }

  /**
   * Constructor for unpartitioned tables
   *
   * @param table Hive table
   */
  public InputPartition(Table table) {
    this(table.getSd());
  }

  /**
   * Constructor
   *
   * @param table Hive table
   * @param partition Partition of table
   */
  public InputPartition(Table table, Partition partition) {
    this(partition.getSd());
    inputSplitData.parsePartitionValues(table, partition);
  }

  /**
   * Internal constructor with storage descriptor
   *
   * @param storageDescriptor StorageDescriptor
   */
  private InputPartition(StorageDescriptor storageDescriptor) {
    inputSplitData = new InputSplitData(storageDescriptor);
    inputFormatClass = Classes.classForName(storageDescriptor.getInputFormat());
    location = storageDescriptor.getLocation();
  }

  /**
   * Create InputFormat for this partition
   *
   * @param conf Configuration
   * @return InputFormat
   */
  public InputFormat makeInputFormat(Configuration conf) {
    return ReflectionUtils.newInstance(inputFormatClass, conf);
  }

  public InputSplitData getInputSplitData() {
    return inputSplitData;
  }

  public Class<? extends InputFormat> getInputFormatClass() {
    return inputFormatClass;
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
