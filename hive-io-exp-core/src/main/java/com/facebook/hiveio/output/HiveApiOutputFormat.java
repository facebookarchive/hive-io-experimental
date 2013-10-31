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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.HackOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.FileSystems;
import com.facebook.hiveio.common.HadoopUtils;
import com.facebook.hiveio.common.HiveUtils;
import com.facebook.hiveio.common.Inspectors;
import com.facebook.hiveio.common.ProgressReporter;
import com.facebook.hiveio.record.HiveWritableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.facebook.hiveio.schema.HiveTableSchemaImpl;
import com.facebook.hiveio.schema.HiveTableSchemas;
import com.facebook.hiveio.table.HiveTables;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hadoop compatible OutputFormat for writing to Hive.
 */
public class HiveApiOutputFormat
    extends OutputFormat<WritableComparable, HiveWritableRecord> {
  /** Default profile if none given */
  public static final String DEFAULT_PROFILE_ID = "output-profile";

  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(HiveApiOutputFormat.class);

  /** Counter for the files created, so we would be able to get unique name for new files */
  private static final AtomicInteger CREATED_FILES_COUNTER = new AtomicInteger(0);

  /** Which profile to lookup */
  private String myProfileId = DEFAULT_PROFILE_ID;

  /**
   * Initialize this output format
   *
   * @param outputDescription Output description
   * @param profileId Profile id
   * @param conf Configuration
   */
  public void initialize(HiveOutputDescription outputDescription, String profileId,
      Configuration conf) {
    checkNotNull(outputDescription, "inputDescription is null");
    checkNotNull(profileId, "profileId is null");
    checkNotNull(conf, "conf is null");
    try {
      setMyProfileId(profileId);
      initProfile(conf, outputDescription, profileId);
      HiveTableSchemas.initTableSchema(conf, profileId, outputDescription.getTableDesc());
    } catch (IOException e) {
      throw new IllegalStateException("initialize: IOException occurred", e);
    }
  }

  public String getMyProfileId() {
    return myProfileId;
  }

  public void setMyProfileId(String myProfileId) {
    this.myProfileId = myProfileId;
  }

  /**
   * Get table schema for this profile in the configuration.
   * @param conf Configuration to lookup in
   * @return HiveTableSchema
   */
  public HiveTableSchema getTableSchema(Configuration conf) {
    return HiveTableSchemas.getFromConf(conf, myProfileId);
  }

  /**
   * Initialize using object's profile ID with Configuration and output
   * description passed in.
   * @param conf Configuration to use
   * @param outputDesc HiveOutputDescription
   * @throws TException Hive Metastore issues
   */
  public void init(Configuration conf, HiveOutputDescription outputDesc)
    throws TException, IOException {
    initProfile(conf, outputDesc, myProfileId);
  }

  /**
   * Initialize with default profile ID using Configuration and output
   * description passsed in.
   * @param conf Configuration to use
   * @param outputDesc HiveOutputDescription
   * @throws TException Hive Metastore issues
   */
  public static void initDefaultProfile(Configuration conf,
    HiveOutputDescription outputDesc) throws TException, IOException {
    initProfile(conf, outputDesc, DEFAULT_PROFILE_ID);
  }

  /**
   * Initialize passed in profile ID with Configuration and output description
   * passed in.
   * @param conf Configuration to use
   * @param outputDesc HiveOutputDescription
   * @param profileId Profile to use
   * @throws IOException Hive Metastore issues
   */
  public static void initProfile(final Configuration conf,
                                 final HiveOutputDescription outputDesc,
                                 final String profileId)
    throws IOException {
    Table table = HiveTables.getTable(conf, profileId, outputDesc);
    sanityCheck(table, outputDesc);

    OutputInfo outputInfo = new OutputInfo(table);

    String partitionPiece;
    if (outputInfo.hasPartitionInfo()) {
      try {
        partitionPiece = HiveUtils.computePartitionPath(
            outputInfo.getPartitionInfo(), outputDesc.getPartitionValues());
      } catch (MetaException e) {
        throw new IOException(e);
      }
    } else {
      partitionPiece = "_temp";
    }
    String partitionPath = outputInfo.getTableRoot() + Path.SEPARATOR + partitionPiece;

    outputInfo.setPartitionPath(partitionPath);
    HadoopUtils.setOutputDir(conf, partitionPath);

    if (outputInfo.hasPartitionInfo()) {
      outputInfo.setFinalOutputPath(outputInfo.getPartitionPath());
    } else {
      outputInfo.setFinalOutputPath(table.getSd().getLocation());
    }

    HiveTableSchema tableSchema = HiveTableSchemaImpl.fromTable(conf, table);
    HiveTableSchemas.putToConf(conf, profileId, tableSchema);

    OutputConf outputConf = new OutputConf(conf, profileId);
    outputConf.writeOutputDescription(outputDesc);
    outputConf.writeOutputTableInfo(outputInfo);

    LOG.info("initProfile '{}' using {}", profileId, outputDesc);
  }

  /**
   * Check table is not misconfigured.
   * @param table Table to check
   * @param outputDesc HiveOutputDescription to use
   */
  private static void sanityCheck(Table table,
                                  HiveOutputDescription outputDesc) {
    StorageDescriptor sd = table.getSd();
    Preconditions.checkArgument(!sd.isCompressed());
    Preconditions.checkArgument(nullOrEmpty(sd.getBucketCols()));
    Preconditions.checkArgument(nullOrEmpty(sd.getSortCols()));
    Preconditions.checkArgument(table.getPartitionKeysSize() ==
        outputDesc.numPartitionValues());
  }

  /**
   * Check if collection is null or empty
   * @param <X> data type
   * @param c Collection to check
   * @return true if collection is null or empty
   */
  private static <X> boolean nullOrEmpty(Collection<X> c) {
    return c == null || c.isEmpty();
  }

  /**
   * Convert partition value map with ordered partition info into list of
   * partition values.
   * @param partitionValues Map of partition data
   * @param fieldSchemas List of partition column definitions
   * @return List<String> of partition values
   */
  private List<String> listOfPartitionValues(
    Map<String, String> partitionValues, List<FieldSchema> fieldSchemas) {
    List<String> values = Lists.newArrayList();
    for (FieldSchema fieldSchema : fieldSchemas) {
      String value = partitionValues.get(fieldSchema.getName().toLowerCase());
      values.add(value);
    }
    return values;
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext)
    throws IOException, InterruptedException
  {
    Configuration conf = jobContext.getConfiguration();
    OutputConf outputConf = new OutputConf(conf, myProfileId);

    HiveOutputDescription description = outputConf.readOutputDescription();
    OutputInfo oti = outputConf.readOutputTableInfo();
    LOG.info("Check output specs of " + description);

    if (description == null) {
      LOG.error("HiveOutputDescription is null in Configuration, nothing to check");
      return;
    }
    checkTableExists(conf, description);

    if (oti == null) {
      LOG.error("OutputInfo is null in Configuration, nothing to check");
      return;
    }
    checkPartitionInfo(conf, description, oti, outputConf);
  }

  /**
   * Check that the table exists
   *
   * @param conf Configuration
   * @param description HiveOutputDescription
   * @throws IOException When table does not exist
   */
  private void checkTableExists(
      final Configuration conf,
      final HiveOutputDescription description)
    throws IOException
  {
    BackoffRetryTask<Boolean> backoffRetryTask =
        new BackoffRetryTask<Boolean>(conf) {
          @Override
          public Boolean idempotentTask() throws TException {
            ThriftHiveMetastore.Iface client =
                description.metastoreClient(conf);
            try {
              client.get_table(description.getTableDesc().getDatabaseName(),
                  description.getTableDesc().getTableName());
            } catch (NoSuchObjectException e) {
              return false;
            }
            return true;
          }
        };
    if (!backoffRetryTask.run()) {
      throw new IOException("Table does not exist");
    }
  }

  /**
   * Check that the table's partition info and the user's match.
   *
   * @param conf Configuration
   * @param description HiveInputDescription
   * @param oti OutputInfo
   * @param outputConf OutputConf
   * @throws IOException
   */
  private void checkPartitionInfo(Configuration conf,
      HiveOutputDescription description, OutputInfo oti, OutputConf outputConf) throws IOException {
    if (oti.hasPartitionInfo()) {
      if (!description.hasPartitionValues()) {
        throw new IOException("table is partitioned but user input isn't");
      }
      if (outputConf.shouldDropPartitionIfExists()) {
        dropPartitionIfExists(conf, description, oti);
      } else {
        checkPartitionDoesntExist(conf, description, oti);
      }
    } else {
      if (description.hasPartitionValues()) {
        throw new IOException("table is not partitioned but user input is");
      } else {
        checkTableIsEmpty(conf, description, oti);
      }
    }
  }

  /**
   * Check if the given table is empty, that is has no files
   * @param conf Configuration to use
   * @param description HiveOutputDescription
   * @param oti OutputInfo
   * @throws IOException Hadoop Filesystem issues
   */
  private void checkTableIsEmpty(Configuration conf,
    HiveOutputDescription description, OutputInfo oti)
    throws IOException {
    Path tablePath = new Path(oti.getTableRoot());
    FileSystem fs = tablePath.getFileSystem(conf);

    if (fs.exists(tablePath) && FileSystems.dirHasNonHiddenFiles(fs, tablePath)) {
      throw new IOException("Table " + description.getTableDesc().getTableName() +
          " has existing data");
    }
  }

  /**
   * Check that partition we will be writing to does not already exist
   * @param conf Configuration to use
   * @param description HiveOutputDescription
   * @param oti OutputInfo
   * @throws IOException Hadoop Filesystem issues
   */
  private void checkPartitionDoesntExist(
      final Configuration conf,
      final HiveOutputDescription description,
      final OutputInfo oti)
    throws IOException
  {
    BackoffRetryTask<Boolean> backoffRetryTask =
        new BackoffRetryTask<Boolean>(conf) {
          @Override
          public Boolean idempotentTask() throws TException {
            ThriftHiveMetastore.Iface client =
                description.metastoreClient(conf);

            String db = description.getTableDesc().getDatabaseName();
            String table = description.getTableDesc().getTableName();

            if (oti.hasPartitionInfo()) {
              Map<String, String> partitionSpec =
                  description.getPartitionValues();
              List<String> partitionValues = listOfPartitionValues(
                  partitionSpec, oti.getPartitionInfo());

              if (partitionExists(client, db, table, partitionValues)) {
                LOG.error("Table " + db + ":" + table + " partition " +
                    partitionSpec + " already exists");
                return true;
              }
            }
            return false;
          }
        };
    if (backoffRetryTask.run()) {
      throw new IOException("Table already exists");
    }
  }

  /**
   * Query Hive metastore if a table's partition exists already.
   * @param client Hive client
   * @param db Hive database name
   * @param table Hive table name
   * @param partitionValues list of partition values
   * @return true if partition exists
   */
  private boolean partitionExists(
      ThriftHiveMetastore.Iface client, String db, String table,
      List<String> partitionValues) {
    List<String> partitionNames;
    try {
      partitionNames = client.get_partition_names_ps(db, table,
          partitionValues, (short) 1);
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      return false;
    }
    return !partitionNames.isEmpty();
  }

  /**
   * Drop partition which we will be writing to
   * @param conf Configuration to use
   * @param description HiveOutputDescription
   * @param oti OutputInfo
   * @return True iff partition was dropped
   */
  private boolean dropPartitionIfExists(Configuration conf,
      HiveOutputDescription description, OutputInfo oti)
    throws IOException
  {
    ThriftHiveMetastore.Iface client;
    try {
      client = description.metastoreClient(conf);
    } catch (TException e) {
      throw new IOException(e);
    }

    String db = description.getTableDesc().getDatabaseName();
    String table = description.getTableDesc().getTableName();

    if (oti.hasPartitionInfo()) {
      Map<String, String> partitionSpec = description.getPartitionValues();
      List<String> partitionValues = listOfPartitionValues(
          partitionSpec, oti.getPartitionInfo());

      if (partitionExists(client, db, table, partitionValues)) {
        LOG.info("Dropping partition {} from table {}:{}", partitionSpec, db, table);
        return dropPartition(client, db, table, partitionValues);
      }
    }
    return false;
  }

  /**
   * Query Hive metastore to drop a partition.
   * @param client Hive client
   * @param db Hive database name
   * @param table Hive table name
   * @param partitionValues list of partition values
   * @return true if partition was dropped
   */
  private boolean dropPartition(
      ThriftHiveMetastore.Iface client, String db, String table,
      List<String> partitionValues) {
    try {
      return client.drop_partition(db, table, partitionValues, true);
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      return false;
    }
  }

  @Override
  public RecordWriterImpl getRecordWriter(TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException
  {
    HadoopUtils.setWorkOutputDir(taskAttemptContext);

    Configuration conf = taskAttemptContext.getConfiguration();
    OutputConf outputConf = new OutputConf(conf, myProfileId);

    OutputInfo oti = outputConf.readOutputTableInfo();

    HiveUtils.setRCileNumColumns(conf, oti.getColumnInfo().size());
    HadoopUtils.setOutputKeyWritableClass(conf, NullWritable.class);

    Serializer serializer = oti.createSerializer(conf);
    HadoopUtils.setOutputValueWritableClass(conf,
        serializer.getSerializedClass());

    org.apache.hadoop.mapred.OutputFormat baseOutputFormat =
        ReflectionUtils.newInstance(oti.getOutputFormatClass(), conf);
    // CHECKSTYLE: stop LineLength
    org.apache.hadoop.mapred.RecordWriter<WritableComparable, Writable> baseWriter =
        getBaseRecordWriter(taskAttemptContext, baseOutputFormat);
    // CHECKSTYLE: resume LineLength

    StructObjectInspector soi = Inspectors.createFor(oti.getColumnInfo());

    if (!outputConf.shouldResetSlowWrites()) {
      return new RecordWriterImpl(baseWriter, serializer, soi);
    } else {
      long writeTimeout = outputConf.getWriteResetTimeout();
      return new ResettableRecordWriterImpl(baseWriter, serializer, soi, taskAttemptContext,
          baseOutputFormat, writeTimeout);
    }
  }

  /**
   * Get the base Hadoop RecordWriter.
   * @param taskAttemptContext TaskAttemptContext
   * @param baseOutputFormat Hadoop OutputFormat
   * @return RecordWriter
   * @throws IOException Hadoop issues
   */
  // CHECKSTYLE: stop LineLengthCheck
  protected static org.apache.hadoop.mapred.RecordWriter<WritableComparable, Writable> getBaseRecordWriter(
    TaskAttemptContext taskAttemptContext,
    org.apache.hadoop.mapred.OutputFormat baseOutputFormat) throws IOException
  {
    // CHECKSTYLE: resume LineLengthCheck
    HadoopUtils.setWorkOutputDir(taskAttemptContext);
    JobConf jobConf = new JobConf(taskAttemptContext.getConfiguration());
    int fileId = CREATED_FILES_COUNTER.incrementAndGet();
    String name = FileOutputFormat.getUniqueName(jobConf, "part-" + fileId);
    Reporter reporter = new ProgressReporter(taskAttemptContext);
    org.apache.hadoop.mapred.RecordWriter<WritableComparable, Writable> baseWriter =
        baseOutputFormat.getRecordWriter(null, jobConf, name, reporter);
    LOG.info("getBaseRecordWriter: Created new {} with file {}", baseWriter, name);
    return baseWriter;
  }

  @Override
  public HiveApiOutputCommitter getOutputCommitter(
    TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException
  {
    HadoopUtils.setWorkOutputDir(taskAttemptContext);
    Configuration conf = taskAttemptContext.getConfiguration();
    JobConf jobConf = new JobConf(conf);
    OutputCommitter baseCommitter = jobConf.getOutputCommitter();
    LOG.info("Getting output committer with base output committer {}",
        baseCommitter.getClass().getSimpleName());
    return new HiveApiOutputCommitter(new HackOutputCommitter(baseCommitter, jobConf), myProfileId);
  }
}
