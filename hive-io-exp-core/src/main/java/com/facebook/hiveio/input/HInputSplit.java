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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.HadoopUtils;
import com.facebook.hiveio.common.ProgressReporter;
import com.facebook.hiveio.common.SerDes;
import com.facebook.hiveio.common.Writables;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.facebook.hiveio.schema.HiveTableSchemaImpl;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * InputSplit for Hive
 */
class HInputSplit extends InputSplit
    implements org.apache.hadoop.mapred.InputSplit, Configurable {
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(HInputSplit.class);

  /** Hadoop InputFormat for reading records */
  private org.apache.hadoop.mapred.InputFormat baseInputFormat;
  /** Hadoop InputSplit */
  private org.apache.hadoop.mapred.InputSplit baseSplit;
  /** Schema of table */
  private final HiveTableSchema tableSchema;
  /** Data for this input split */
  private final InputSplitData inputSplitData;
  /** Which columns to read */
  private int[] columnIds;

  // These members are not serialized.
  /** Hadoop Configuration */
  private Configuration conf;
  /** Hive Deserializer */
  private Deserializer deserializer;

  /**
   * Constructor for reflection
   */
  public HInputSplit() {
    tableSchema = new HiveTableSchemaImpl();
    inputSplitData = new InputSplitData();
    columnIds = new int[0];
  }

  /**
   * Constructor
   *
   * @param baseInputFormat Hadoop InputFormat
   * @param baseSplit Hadoop InputSplit
   * @param tableSchema Schema for Hive table
   * @param columnIds List of column ids
   * @param inputSplitData Data for this split
   * @param conf Configuration
   */
  public HInputSplit(org.apache.hadoop.mapred.InputFormat baseInputFormat,
                     org.apache.hadoop.mapred.InputSplit baseSplit,
                     HiveTableSchema tableSchema, int[] columnIds,
                     InputSplitData inputSplitData, Configuration conf) {
    this.baseSplit = baseSplit;
    this.baseInputFormat = baseInputFormat;
    this.tableSchema = tableSchema;
    this.columnIds = columnIds;
    this.inputSplitData = inputSplitData;
    setConf(conf);
  }

  public Deserializer getDeserializer() {
    return Preconditions.checkNotNull(deserializer);
  }

  /**
   * Get array of partition values
   *
   * @return partition values array
   */
  public String[] getPartitionValues() {
    List<String> partValues = inputSplitData.getPartitionValues();
    return partValues.toArray(new String[partValues.size()]);
  }

  public HiveTableSchema getTableSchema() {
    return tableSchema;
  }

  public int[] getColumnIds() {
    return columnIds;
  }

  /**
   * GEt the Hadoop RecordReader which we will wrap with ours.
   *
   * @param jobConf Hadoop Job configuration
   * @param progressable Object tracking progress
   * @return underlying RecordReader
   * @throws IOException I/O errors
   */
  public org.apache.hadoop.mapred.RecordReader<WritableComparable, Writable>
  getBaseRecordReader(JobConf jobConf, Progressable progressable)
    throws IOException {
    Reporter reporter = new ProgressReporter(progressable);
    return baseInputFormat.getRecordReader(baseSplit, jobConf, reporter);
  }

  /**
   * Initialize Deserializer used
   */
  private void initDeserializer() {
    if (deserializer == null && inputSplitData.hasDeserializerClass()) {
      deserializer = inputSplitData.createDeserializer();
    }
    if (deserializer != null && conf != null) {
      SerDes.initDeserializer(deserializer, conf,
          inputSplitData.getColumnInfo(),
          inputSplitData.getDeserializerParams());
    }
  }

  @Override
  public long getLength() throws IOException {
    return baseSplit.getLength();
  }

  @Override
  public String[] getLocations() throws IOException {
    return baseSplit.getLocations();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    HadoopUtils.setConfIfPossible(baseInputFormat, conf);
    HadoopUtils.setConfIfPossible(baseSplit, conf);
    HadoopUtils.setConfIfPossible(deserializer, conf);
    initDeserializer();
  }

  @Override
  public Configuration getConf() {
    if (conf == null) {
      getConfFromMembers();
    }
    return conf;
  }

  /**
   * Get Configuration object from member variables
   */
  private void getConfFromMembers() {
    Object[] objectsToCheck = new Object[] {
      baseSplit,
      baseInputFormat,
      deserializer
    };
    for (int i = 0; conf == null && i < objectsToCheck.length; ++i) {
      conf = HadoopUtils.getConfIfPossible(objectsToCheck[i]);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Writables.writeUnknownWritable(out, baseSplit);
    Writables.writeClassName(out, baseInputFormat);
    tableSchema.write(out);
    inputSplitData.write(out);
    Writables.writeIntArray(out, columnIds);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    baseSplit = Writables.readUnknownWritable(in);
    baseInputFormat = Writables.readNewInstance(in);
    HadoopUtils.configureInputFormat(baseInputFormat, conf);
    tableSchema.readFields(in);
    inputSplitData.readFields(in);
    columnIds = Writables.readIntArray(in);
    initDeserializer();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("baseInputFormat class", baseInputFormat.getClass())
        .add("tableSchema", tableSchema)
        .add("inputSplitData", inputSplitData)
        .add("columnIds", Arrays.toString(columnIds))
        .add("baseSplitClass", baseSplit.getClass())
        .add("baseSplit", baseSplit)
        .toString();
  }
}
