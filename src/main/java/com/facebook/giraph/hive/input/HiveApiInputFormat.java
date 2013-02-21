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

package com.facebook.giraph.hive.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.facebook.giraph.hive.HiveRecord;
import com.facebook.giraph.hive.HiveTableSchema;
import com.facebook.giraph.hive.HiveTableSchemas;
import com.facebook.giraph.hive.impl.HiveApiTableSchema;
import com.facebook.giraph.hive.impl.common.HadoopUtils;
import com.facebook.giraph.hive.impl.common.HiveMetastores;
import com.facebook.giraph.hive.impl.common.HiveUtils;
import com.facebook.giraph.hive.impl.input.HiveApiInputSplit;
import com.facebook.giraph.hive.impl.input.HiveApiRecordReader;
import com.facebook.giraph.hive.impl.input.InputConf;
import com.facebook.giraph.hive.impl.input.InputInfo;
import com.facebook.giraph.hive.impl.input.InputPartition;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Lists.transform;

/**
 * InputFormat to read from Hive
 */
public class HiveApiInputFormat
    extends InputFormat<WritableComparable, HiveRecord> {
  /** Logger */
  public static final Logger LOG = Logger.getLogger(HiveApiInputFormat.class);

  /** Configuration key for whether to reuse records */
  public static final String REUSE_RECORD_KEY = "hive.api.input.reuse_record";

  /** Default profile ID if none given */
  public static final String DEFAULT_PROFILE_ID = "input-profile";

  /** Which profile to lookup */
  private String myProfileId = DEFAULT_PROFILE_ID;

  /** Input observer */
  private HiveApiInputObserver observer;

  /**
   * Get profile id
   * @return integer profile id
   */
  public String getMyProfileId() {
    return myProfileId;
  }

  /**
   * Set profile ID
   * @param myProfileId integer profile id
   */
  public void setMyProfileId(String myProfileId) {
    this.myProfileId = myProfileId;
  }

  /**
   * Get observer currently registered
   * @return Observer
   */
  public HiveApiInputObserver getObserver() {
    return observer;
  }

  /**
   * Set observer that will watch operations
   * @param observer Observer to set
   */
  public void setObserver(HiveApiInputObserver observer) {
    this.observer = observer;
  }

  /**
   * Get table schema for this profile
   * @param conf Configuration to use
   * @return HiveTableSchema
   */
  public HiveTableSchema getTableSchema(Configuration conf) {
    return HiveTableSchemas.getForProfile(conf, myProfileId);
  }

  /**
   * Set information to lookup later.
   *
   * @param conf Hadoop Configuration to store data in
   * @param hiveInputDescription TableDesc of what to lookup in Hive
   * @throws TException If anything goes wrong contacting Metastore
   */
  public static void initDefaultProfile(Configuration conf,
    HiveInputDescription hiveInputDescription)
    throws TException {
    initProfile(conf, hiveInputDescription, DEFAULT_PROFILE_ID);
  }

  /**
   * Set information to lookup later. This allows for multiple configurations by
   * using a single profile ID for each set of table information.
   *
   * @param conf Hadoop Configuration to store data in
   * @param inputDesc TableDesc of what to lookup in Hive
   * @param profileId String profile of configuration to set, allowing for
   *                  multiple configs
   * @throws TException If anything goes wrong contacting Metastore
   */
  public static void initProfile(
    Configuration conf, HiveInputDescription inputDesc, String profileId)
    throws TException {
    HiveConf hiveConf = new HiveConf(conf, HiveApiInputFormat.class);
    ThriftHiveMetastore.Iface client = HiveMetastores.create(hiveConf);
    initProfile(conf, inputDesc, profileId, client);
  }

  /**
   * Set information to lookup later. This allows for multiple configurations by
   * using a single profile ID for each set of table information.
   * This version allows you to supply your own hive metastore thrift client.
   *
   * @param conf Hadoop Configuration to store data in
   * @param inputDesc TableDesc of what to lookup in Hive
   * @param profileId String profile of configuration to set, allowing for
   *                  multiple configs
   * @param client Thrift client to use.
   * @throws TException If anything goes wrong contacting Metastore
   */
  public static void initProfile(Configuration conf,
    HiveInputDescription inputDesc, String profileId,
    ThriftHiveMetastore.Iface client) throws TException {
    String dbName = inputDesc.getDbName();
    String tableName = inputDesc.getTableName();

    Table table;
    try {
      table = client.get_table(dbName, tableName);
    } catch (NoSuchObjectException e) {
      throw new TException(e);
    } catch (MetaException e) {
      throw new TException(e);
    }

    final HiveTableSchema tableSchema = HiveApiTableSchema.fromTable(table);
    HiveTableSchemas.putForName(conf, dbName, tableName, tableSchema);
    HiveTableSchemas.putForProfile(conf, profileId, tableSchema);

    Function<String, Integer> columnNameToId = new Function<String, Integer>() {
      @Override public Integer apply(String input) {
        return tableSchema.positionOf(input);
      }
    };
    List<Integer> columnIds = transform(inputDesc.getColumns(), columnNameToId);

    InputInfo inputInfo = new InputInfo(tableSchema, columnIds);

    if (table.getPartitionKeysSize() == 0) {
      // table without partitions
      inputInfo.addPartition(new InputPartition(table));
    } else {
      // table with partitions, find matches to user filter.
      List<Partition> partitions = null;
      try {
        partitions = client.get_partitions_by_filter(dbName, tableName,
            inputDesc.getPartitionFilter(), (short) -1);
      } catch (NoSuchObjectException e) {
        throw new TException(e.getMessage());
      } catch (MetaException e) {
        throw new TException(e);
      }
      for (Partition partition : partitions) {
        inputInfo.addPartition(new InputPartition(table, partition));
      }
    }

    InputConf inputConf = new InputConf(conf, profileId);
    inputConf.writeNumSplitsToConf(inputDesc.getNumSplits());
    inputConf.writeProfileIdToConf();
    inputConf.writeInputInfoToConf(inputInfo);

    LOG.info("initProfile '" + profileId + "' to " + inputDesc);
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext)
    throws IOException, InterruptedException {
    Configuration conf = jobContext.getConfiguration();

    InputConf inputConf = new InputConf(conf, myProfileId);

    LOG.info("getSplits for profile " + inputConf.getProfileId());

    JobConf jobConf = new JobConf(conf);
    InputInfo inputInfo = inputConf.readInputInfoFromConf();

    int partitionNum = 0;
    List<InputSplit> splits = Lists.newArrayList();
    Iterable<InputPartition> partitions = inputInfo.getPartitions();

    for (InputPartition inputPartition : partitions) {
      org.apache.hadoop.mapred.InputFormat baseInputFormat =
          inputPartition.makeInputFormat(conf);
      HadoopUtils.setInputDir(jobConf, inputPartition.getLocation());

      int splitsRequested = inputConf.readNumSplitsFromConf();
      org.apache.hadoop.mapred.InputSplit[] baseSplits =
          baseInputFormat.getSplits(jobConf, splitsRequested);
      LOG.info("Requested " + splitsRequested + " from partition (" +
          partitionNum + " out of " + Iterables.size(partitions) +
          ") values: " +
          inputPartition.getInputSplitData().getPartitionValues() +
          ", got " + baseSplits.length + " splits");

      for (org.apache.hadoop.mapred.InputSplit baseSplit : baseSplits)  {
        InputSplit split = new HiveApiInputSplit(baseInputFormat, baseSplit,
            inputInfo.getTableSchema(), inputInfo.getColumnIds(),
            inputPartition.getInputSplitData(), conf);
        splits.add(split);
      }

      partitionNum++;
    }

    return splits;
  }

  @Override
  public RecordReader<WritableComparable, HiveRecord>
  createRecordReader(InputSplit inputSplit, TaskAttemptContext context)
    throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    JobConf jobConf = new JobConf(conf);

    HiveApiInputSplit apiInputSplit;
    if (inputSplit instanceof HiveApiInputSplit) {
      apiInputSplit = (HiveApiInputSplit) inputSplit;
    } else {
      throw new IllegalArgumentException("InputSplit not a HiveApiInputSplit");
    }
    apiInputSplit.setConf(jobConf);

    // CHECKSTYLE: stop LineLength
    org.apache.hadoop.mapred.RecordReader<WritableComparable, Writable> baseRecordReader =
        apiInputSplit.getBaseRecordReader(jobConf, context);
    // CHECKSTYLE: resume LineLength

    HiveUtils.setReadColumnIds(conf, apiInputSplit.getColumnIds());

    boolean reuseRecord = conf.getBoolean(REUSE_RECORD_KEY, true);

    HiveApiRecordReader reader = new HiveApiRecordReader(
        baseRecordReader,
        apiInputSplit.getDeserializer(),
        apiInputSplit.getPartitionValues(),
        apiInputSplit.getTableSchema().numColumns(),
        reuseRecord);
    reader.setObserver(observer);

    return reader;
  }
}
