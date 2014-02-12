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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.Classes;
import com.facebook.hiveio.common.SerDes;
import com.facebook.hiveio.common.Writables;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Holds information for Hive output
 */
public class OutputInfo implements Writable {
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(OutputInfo.class);

  /** Parameters for Hive table */
  private final Map<String, String> tableParams;

  /** Class for writing */
  private Class<? extends OutputFormat> outputFormatClass;

  /** Partition column information */
  private final List<FieldSchema> partitionInfo;
  /** Regular column information */
  private final List<FieldSchema> columnInfo;

  /** Class used for serialization */
  private Class<? extends SerDe> serializerClass;
  /** Parameters for serializer */
  private final Map<String, String> serializerParams;

  /** Path to table root in Hadoop */
  private String tableRoot;
  /**
   * Path to specific partition we're writing. If not partitioned, this will be
   * the same as tablePath
   */
  private String partitionPath;
  /**
   * Path to where we're writing to. If partitioned this is the same as
   * partitionPath, otherwise this is a temporary path.
   */
  private String finalOutputPath;

  /**
   * Default constructor
   */
  public OutputInfo() {
    this.tableParams = Maps.newHashMap();
    this.partitionInfo = Lists.newArrayList();
    this.columnInfo = Lists.newArrayList();
    this.serializerClass = null;
    this.serializerParams = Maps.newHashMap();
  }

  /**
   * Construct from Hive table
   * @param table Hive table to grab information from
   */
  public OutputInfo(Table table) {
    partitionInfo = table.getPartitionKeys();

    StorageDescriptor storageDescriptor = table.getSd();
    tableParams = table.getParameters();
    outputFormatClass = Classes.classForName(storageDescriptor.getOutputFormat());
    columnInfo = storageDescriptor.getCols();
    tableRoot = storageDescriptor.getLocation();

    SerDeInfo serDeInfo = storageDescriptor.getSerdeInfo();
    serializerClass = SerDes.getSerDeClass(serDeInfo);
    serializerParams = serDeInfo.getParameters();
  }

  public String getTableRoot() {
    return tableRoot;
  }

  public List<FieldSchema> getColumnInfo() {
    return columnInfo;
  }

  public Map<String, String> getTableParams() {
    return tableParams;
  }

  public Class<? extends OutputFormat> getOutputFormatClass() {
    return outputFormatClass;
  }

  public Class<? extends Serializer> getSerializerClass() {
    return serializerClass;
  }

  public Map<String, String> getSerializerParams() {
    return serializerParams;
  }

  public List<FieldSchema> getPartitionInfo() {
    return partitionInfo;
  }

  /**
   * Check if this table has any partition info
   * @return true if we have partition information
   */
  public boolean hasPartitionInfo() {
    return partitionInfo != null && !partitionInfo.isEmpty();
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  /**
   * Set partition path
   *
   * @param partitionPath path to partition data
   * @return this
   */
  public OutputInfo setPartitionPath(String partitionPath) {
    LOG.info("Setting partition path to {}", partitionPath);
    this.partitionPath = partitionPath;
    return this;
  }

  public String getFinalOutputPath() {
    return finalOutputPath;
  }

  /**
   *  Set final output path
   *
   * @param finalOutputPath path to final result
   * @return this
   */
  public OutputInfo setFinalOutputPath(String finalOutputPath) {
    LOG.info("Setting final output path to {}", finalOutputPath);
    this.finalOutputPath = finalOutputPath;
    return this;
  }

  /**
   * Create Serializer using Configuration passed in
   *
   * @param conf Configuration to use
   * @return A new, configured, Serializer
   */
  public Serializer createSerializer(Configuration conf) {
    Serializer serializer = ReflectionUtils.newInstance(serializerClass, conf);
    SerDes.initSerializer(serializer, conf, columnInfo, serializerParams);
    return serializer;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Writables.writeClassName(out, Preconditions.checkNotNull(outputFormatClass));
    Writables.writeFieldSchemas(out, partitionInfo);
    Writables.writeStrStrMap(out, tableParams);
    Writables.writeFieldSchemas(out, columnInfo);
    WritableUtils.writeString(out, Preconditions.checkNotNull(tableRoot));
    WritableUtils.writeString(out, Preconditions.checkNotNull(partitionPath));
    WritableUtils.writeString(out, Preconditions.checkNotNull(finalOutputPath));
    Writables.writeClassName(out, Preconditions.checkNotNull(serializerClass));
    Writables.writeStrStrMap(out, serializerParams);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    outputFormatClass = Writables.readClass(in);
    Writables.readFieldSchemas(in, partitionInfo);
    Writables.readStrStrMap(in, tableParams);
    Writables.readFieldSchemas(in, columnInfo);
    tableRoot = WritableUtils.readString(in);
    partitionPath = WritableUtils.readString(in);
    finalOutputPath = WritableUtils.readString(in);
    serializerClass = Writables.readClass(in);
    Writables.readStrStrMap(in, serializerParams);
  }
}
