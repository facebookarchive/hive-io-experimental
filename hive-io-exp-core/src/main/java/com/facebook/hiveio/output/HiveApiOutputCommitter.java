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

import com.facebook.hiveio.common.BackoffRetryTask;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TException;
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
  /** Logger */
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
    baseCommitter.setupJob(jobContext);
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException
  {
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
  private static void writeSuccessFile(Configuration conf) throws IOException
  {
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
  private void registerPartitions(
      final Configuration conf,
      final HiveOutputDescription outputDesc,
      final OutputInfo outputInfo) throws IOException
  {
    BackoffRetryTask<Void> backoffRetryTask = new BackoffRetryTask<Void>(conf) {
      @Override
      public Void idempotentTask() throws TException {
        String dbName = outputDesc.getTableDesc().getDatabaseName();
        String tableName = outputDesc.getTableDesc().getTableName();

        ThriftHiveMetastore.Iface client = outputDesc.metastoreClient(conf);
        Table hiveTable = client.get_table(dbName, tableName);

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
        } catch (AlreadyExistsException e) {
          LOG.info("Partition already exists; Giraph must have just created it");
        } catch (InvalidObjectException e) {
          throw new IllegalStateException(e);
        }
        return null;
      }
    };
    backoffRetryTask.run();
  }

  /**
   * Table has no partitions, just copy data
   *
   * @param conf Configuration
   * @param outputInfo OutputInfo
   * @throws IOException I/O errors
   */
  private void noPartitionsCopyData(Configuration conf, OutputInfo outputInfo)
    throws IOException
  {
    Preconditions.checkArgument(!outputInfo.hasPartitionInfo());
    Path tablePath = new Path(outputInfo.getTableRoot());
    Path writePath = new Path(outputInfo.getPartitionPath());
    FileSystem tableFs = tablePath.getFileSystem(conf);
    FileSystem writePathFs = writePath.getFileSystem(conf);
    if (!tableFs.getUri().equals(writePathFs.getUri())) {
      LOG.error("Table's root path fs {} is not on same as its partition path fs {}",
          tableFs.getUri(), writePathFs.getUri());
      throw new IllegalStateException("Table's root path fs " + tableFs.getUri() +
          " is not on same as its partition path fs " + writePathFs.getUri());
    }
    LOG.info("No partitions, copying data from {} to {}", writePath, tablePath);
    FileSystems.move(tableFs, writePath, writePath, tablePath);
    tableFs.delete(writePath, true);
  }

  @Override @Deprecated
  public void cleanupJob(JobContext jobContext) throws IOException
  {
    baseCommitter.cleanupJob(jobContext);
  }

  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state)
    throws IOException
  {
    baseCommitter.abortJob(jobContext, state);
    HadoopUtils.deleteOutputDir(jobContext.getConfiguration());
  }

  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException
  {
    baseCommitter.setupTask(taskContext);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext)
    throws IOException
  {
    return baseCommitter.needsTaskCommit(taskContext);
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException
  {
    HadoopUtils.setWorkOutputDir(taskContext);
    baseCommitter.commitTask(taskContext);
  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException
  {
    baseCommitter.abortTask(taskContext);
  }
}
