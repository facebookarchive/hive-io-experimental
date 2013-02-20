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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.api.HiveTableSchema;
import org.apache.hadoop.hive.api.impl.HiveApiTableSchema;
import org.apache.hadoop.hive.api.impl.common.HadoopUtils;
import org.apache.hadoop.hive.api.impl.common.ProgressReporter;
import org.apache.hadoop.hive.api.impl.common.SerDes;
import org.apache.hadoop.hive.api.impl.common.Writables;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * InputSplit for Hive
 */
public class HiveApiInputSplit extends InputSplit
    implements org.apache.hadoop.mapred.InputSplit, Configurable {
  /** Logger */
  public static final Logger LOG = Logger.getLogger(HiveApiInputSplit.class);

  /** Hadoop InputFormat for reading records */
  private org.apache.hadoop.mapred.InputFormat baseInputFormat;
  /** Hadoop InputSplit */
  private org.apache.hadoop.mapred.InputSplit baseSplit;
  /** Schema of table */
  private final HiveTableSchema tableSchema;
  /** Data for this input split */
  private final InputSplitData inputSplitData;
  /** Which columns to read */
  private final List<Integer> columnIds;

  // These members are not serialized.
  /** Hadoop Configuration */
  private Configuration conf;
  /** Hive Deserializer */
  private Deserializer deserializer;

  /**
   * Constructor for reflection
   */
  public HiveApiInputSplit() {
    tableSchema = new HiveApiTableSchema();
    inputSplitData = new InputSplitData();
    columnIds = Lists.newArrayList();
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
  public HiveApiInputSplit(
      org.apache.hadoop.mapred.InputFormat baseInputFormat,
      org.apache.hadoop.mapred.InputSplit baseSplit,
      HiveTableSchema tableSchema, List<Integer> columnIds,
      InputSplitData inputSplitData, Configuration conf) {
    this.baseSplit = baseSplit;
    this.baseInputFormat = baseInputFormat;
    this.tableSchema = tableSchema;
    this.columnIds = columnIds;
    this.inputSplitData = inputSplitData;
    setConf(conf);
  }

  public org.apache.hadoop.mapred.InputFormat getBaseInputFormat() {
    return baseInputFormat;
  }

  public Deserializer getDeserializer() {
    return Preconditions.checkNotNull(deserializer);
  }

  public List<FieldSchema> getColumnInfo() {
    return inputSplitData.getColumnInfo();
  }

  public Map<String, String> getPartitionValues() {
    return inputSplitData.getPartitionValues();
  }

  public Map<String, String> getDeserializerParams() {
    return inputSplitData.getDeserializerParams();
  }

  public HiveTableSchema getTableSchema() {
    return tableSchema;
  }

  public List<Integer> getColumnIds() {
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
    Object[] objectsToCheck = new Object[] {
      baseSplit,
      baseInputFormat,
      deserializer
    };
    for (int i = 0; conf == null && i < objectsToCheck.length; ++i) {
      conf = HadoopUtils.getConfIfPossible(objectsToCheck[i]);
    }
    return conf;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Writables.writeUnknownWritable(out, baseSplit);
    Writables.writeClassName(out, baseInputFormat);
    tableSchema.write(out);
    inputSplitData.write(out);
    Writables.writeIntList(out, columnIds);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    baseSplit = Writables.readUnknownWritable(in);
    baseInputFormat = Writables.readNewInstance(in);
    tableSchema.readFields(in);
    inputSplitData.readFields(in);
    Writables.readIntList(in, columnIds);
    initDeserializer();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("baseInputFormat class", baseInputFormat.getClass())
        .add("baseSplit class", baseSplit.getClass())
        .add("tableSchema", tableSchema)
        .add("inputSplitData", inputSplitData)
        .add("columnIds", columnIds)
        .toString();
  }
}
