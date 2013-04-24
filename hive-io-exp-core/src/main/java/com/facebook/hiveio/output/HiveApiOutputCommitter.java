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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.FileSystems;
import com.facebook.hiveio.common.HadoopUtils;
import com.facebook.hiveio.common.HiveUtils;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.List;

/**
 * OutputCommitter for Hive output
 */
class HiveApiOutputCommitter extends OutputCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(HiveApiOutputCommitter.class);

  /** Profile ID to use */
  private final String profileId;

  /** Base Hadoop output committer */
  private final OutputCommitter baseCommitter;

  /**
   * Constructor
   *
   * @param baseCommitter Base Hadoop committer
   * @param profileId Profile ID
   */
  public HiveApiOutputCommitter(OutputCommitter baseCommitter, String profileId) {
    this.baseCommitter = baseCommitter;
    this.profileId = profileId;
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException
  {
    LOG.info("OutputCommitter::setupJob");
    baseCommitter.setupJob(jobContext);
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException
  {
    LOG.info("OutputCommitter::commitJob");
    baseCommitter.commitJob(jobContext);

    Configuration conf = jobContext.getConfiguration();
    OutputConf outputConf = new OutputConf(conf, profileId);
    HiveOutputDescription outputDesc = outputConf.readOutputDescription();
    OutputInfo outputInfo = outputConf.readOutputTableInfo();
    if (outputInfo.hasPartitionInfo()) {
      registerPartitions(conf, outputDesc, outputInfo);
    } else {
      noPartitionsCopyData(conf, outputInfo);
    }

    writeSuccessFile(conf);
  }

  /**
   * Write success file to Hadoop if required
   *
   * @param conf Configuration
   * @throws IOException I/O errors
   */
  private static void writeSuccessFile(Configuration conf) throws IOException {
    if (!HadoopUtils.needSuccessMarker(conf)) {
      return;
    }
    Path outputPath = HadoopUtils.getOutputPath(conf);
    FileSystem fs = outputPath.getFileSystem(conf);
    if (fs.exists(outputPath)) {
      Path successPath = new Path(outputPath, "_SUCCESS");
      if (!fs.exists(successPath)) {
        LOG.info("Writing success file to {}", successPath);
        fs.create(successPath).close();
      }
    }
  }

  /**
   * Register partitions for new data we wrote.
   *
   * @param conf Configuration
   * @param outputDesc Output description from user
   * @param outputInfo Internal output information
   * @throws IOException
   */
  private void registerPartitions(Configuration conf, HiveOutputDescription outputDesc,
      OutputInfo outputInfo) throws IOException
  {
    String dbName = outputDesc.getDbName();
    String tableName = outputDesc.getTableName();

    ThriftHiveMetastore.Iface client;
    Table hiveTable;
    try {
      client = outputDesc.metastoreClient(conf);
      hiveTable = client.get_table(dbName, tableName);
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      throw new IOException(e);
    }

    Partition partition = new Partition();
    partition.setDbName(dbName);
    partition.setTableName(tableName);
    partition.setParameters(outputInfo.getTableParams());
    List<String> partitionValues = HiveUtils.orderedPartitionValues(
        hiveTable.getPartitionKeys(), outputDesc.getPartitionValues());
    partition.setValues(partitionValues);

    StorageDescriptor sd = new StorageDescriptor(hiveTable.getSd());
    sd.setParameters(outputInfo.getSerializerParams());
    sd.setLocation(outputInfo.getFinalOutputPath());
    sd.setCols(outputInfo.getColumnInfo());
    partition.setSd(sd);

    LOG.info("Registering partition with values {} located at {}",
        outputInfo.getSerializerParams(), outputInfo.getFinalOutputPath());
    try {
      client.add_partition(partition);
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      throw new IOException(e);
    }
  }

  /**
   * Table has no partitions, just copy data
   *
   * @param conf Configuration
   * @param oti OutputInfo
   * @throws IOException I/O errors
   */
  private void noPartitionsCopyData(Configuration conf, OutputInfo oti)
    throws IOException {
    Preconditions.checkArgument(!oti.hasPartitionInfo());
    Path tablePath = new Path(oti.getTableRoot());
    Path writePath = new Path(oti.getPartitionPath());
    FileSystem tableFs = tablePath.getFileSystem(conf);
    FileSystem writePathFs = writePath.getFileSystem(conf);
    if (!tableFs.getUri().equals(writePathFs.getUri())) {
      LOG.error("Table's root path fs {} is not on same as its partition path fs {}",
          tableFs.getUri(), writePathFs.getUri());
      throw new IllegalStateException("Table's root path fs " + tableFs.getUri() +
          " is not on same as its partition path fs " + writePathFs.getUri());
    }
    LOG.info("No partitions, just copying data from {} to {}", writePath, tablePath);
    FileSystems.move(tableFs, writePath, writePath, tablePath);
    tableFs.delete(writePath, true);
  }

  @Override @Deprecated
  public void cleanupJob(JobContext jobContext) throws IOException
  {
    LOG.info("OutputCommitter::cleanupJob");
    baseCommitter.cleanupJob(jobContext);
  }

  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state)
    throws IOException
  {
    LOG.info("OutputCommitter::abortJob");
    baseCommitter.abortJob(jobContext, state);
    HadoopUtils.deleteOutputDir(jobContext.getConfiguration());
  }

  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException
  {
    LOG.info("OutputCommitter::setupTask");
    baseCommitter.setupTask(taskContext);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext)
    throws IOException
  {
    LOG.info("OutputCommitter::needsTaskCommit");
    return baseCommitter.needsTaskCommit(taskContext);
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException
  {
    LOG.info("OutputCommitter::commitTask");
    HadoopUtils.setWorkOutputDir(taskContext);
    baseCommitter.commitTask(taskContext);
  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException
  {
    LOG.info("OutputCommitter::abortTask");
    baseCommitter.abortTask(taskContext);
  }
}
