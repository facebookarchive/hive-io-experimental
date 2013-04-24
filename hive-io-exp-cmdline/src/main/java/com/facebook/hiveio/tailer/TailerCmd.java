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
package com.facebook.hiveio.tailer;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.HadoopNative;
import com.facebook.hiveio.common.HiveMetastores;
import com.facebook.hiveio.common.HiveStats;
import com.facebook.hiveio.common.HiveTableName;
import com.facebook.hiveio.common.HiveUtils;
import com.facebook.hiveio.input.HiveApiInputFormat;
import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.options.BaseCmd;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.facebook.hiveio.schema.HiveTableSchemas;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.command.Command;
import io.airlift.command.Help;

import javax.inject.Inject;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

import static com.facebook.hiveio.input.HiveApiInputFormat.DEFAULT_PROFILE_ID;

@Command(name = "tail", description = "Dump a Hive table")
public class TailerCmd extends BaseCmd
{
  @Inject TailerArgs args = new TailerArgs();

  public RecordPrinter recordPrinter;

  public void chooseRecordPrinter() {
    if (args.recordBufferFlush > 1) {
      recordPrinter = new BufferedRecordPrinter();
    } else {
      recordPrinter = new DefaultRecordPrinter();
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(TailerCmd.class);

  @Override
  public void execute() throws Exception
  {
    chooseRecordPrinter();

    HadoopNative.requireHadoopNative();

    HostPort metastoreHostPort = getHostPort();
    if (metastoreHostPort == null) {
      return;
    }

    LOG.info("Creating Hive client for Metastore at {}", metastoreHostPort);
    ThriftHiveMetastore.Iface client = HiveMetastores
        .create(metastoreHostPort.host, metastoreHostPort.port);

    HiveInputDescription inputDesc = new HiveInputDescription();
    inputDesc.setDbName(args.inputTable.database);
    inputDesc.setTableName(args.inputTable.table);
    inputDesc.setPartitionFilter(args.inputTable.partitionFilter);
    if (args.requestNumSplits == 0) {
      args.requestNumSplits = args.multiThread.threads * args.requestSplitsPerThread;
    }
    inputDesc.setNumSplits(args.requestNumSplits);

    HiveStats hiveStats = HiveUtils.statsOf(client, inputDesc);
    LOG.info("{}", hiveStats);

    HiveConf hiveConf = new HiveConf(TailerCmd.class);
    args.inputTable.process(hiveConf);

    LOG.info("Setting up input using {}", inputDesc);
    HiveApiInputFormat
        .setProfileInputDesc(hiveConf, inputDesc, DEFAULT_PROFILE_ID);

    HiveApiInputFormat hapi = new HiveApiInputFormat();
    hapi.setMyProfileId(DEFAULT_PROFILE_ID);

    List<InputSplit> splits = hapi.getSplits(hiveConf, client);
    LOG.info("Have {} splits to read", splits.size());

    HiveTableName hiveTableName = new HiveTableName(args.inputTable.database, args.inputTable.table);
    HiveTableSchema schema = HiveTableSchemas.lookup(client, hiveTableName);

    Stats stats = Stats.get(hiveStats);
    Context context = new Context(hapi, hiveConf, schema, hiveStats, stats);
    long startNanos = System.nanoTime();

    if (args.multiThread.isSingleThreaded()) {
      context.splitsQueue = Queues.newArrayDeque(splits);
      readSplits(context);
    } else {
      context.splitsQueue = Queues.newConcurrentLinkedQueue(splits);
      multiThreaded(context, args.multiThread.threads);
    }

    long timeNanos = System.nanoTime() - startNanos;
    if (args.appendStatsTo != null) {
      OutputStream out = new FileOutputStream(args.appendStatsTo, true);
      stats.printEndBenchmark(context, args, timeNanos, out);
    }

    System.err.println("Finished.");
    if (metricsOpts.stderrEnabled()) {
      metricsOpts.dumpMetricsToStderr();
    }
  }

  private void multiThreaded(final Context context, int numThreads)
  {
    List<Thread> threads = Lists.newArrayList();
    for (int i = 0; i < numThreads; ++i) {
      Thread thread = new Thread(new Runnable() {
        @Override public void run() {
          readSplits(context);
        }
      });
      thread.setName("readSplit-" + i);
      thread.start();

      threads.add(thread);
    }

    for (int i = 0; i < threads.size(); ++i) {
      Uninterruptibles.joinUninterruptibly(threads.get(i));
    }
  }

  private void readSplits(Context context)
  {
    while (context.hasMoreSplitsToRead(args.limit)) {
      InputSplit split = context.splitsQueue.poll();
      try {
        readSplit(split, context);
      } catch (Exception e) {
        LOG.error("Failed to read split {}", split, e);
      }
    }
    context.perThread.get().flushBuffer();
  }

  private void readSplit(InputSplit split, Context context)
      throws IOException, InterruptedException
  {
    TaskAttemptID taskId = new TaskAttemptID();
    TaskAttemptContext taskContext = new TaskAttemptContext(context.hiveConf, taskId);
    RecordReader<WritableComparable, HiveReadableRecord> recordReader;
    recordReader = context.hiveApiInputFormat.createRecordReader(split, taskContext);
    recordReader.initialize(split, taskContext);

    int rowsParsed = 0;
    while (recordReader.nextKeyValue() && !context.limitReached(args.limit)) {
      HiveReadableRecord record = recordReader.getCurrentValue();
      if (args.parseOnly) {
        parseRecord(record, context.schema.numColumns());
      } else {
        recordPrinter.printRecord(record, context.schema.numColumns(), context, args);
      }
      ++rowsParsed;
      if (context.rowsParsed.incrementAndGet() >= args.limit) {
        break;
      }
      if (rowsParsed % metricsOpts.updateRows == 0) {
        context.stats.addRows(metricsOpts.updateRows);
        rowsParsed = 0;
      }
    }
    context.stats.addRows(rowsParsed);
  }

  private static void parseRecord(HiveReadableRecord record, int numColumns) {
    for (int index = 0; index < numColumns; ++index) {
      record.get(index);
    }
  }

  private HostPort getHostPort() throws IOException {
    if (args.clustersFile == null) {
      return new HostPort(args.metastore.host, args.metastore.port);
    }
    return clusterFromConfig();
  }

  private HostPort clusterFromConfig() throws IOException {
    if (args.clustersFile == null) {
      LOG.error("Cluster file not given");
      new Help().run();
      return null;
    }
    ObjectMapper objectMapper = new ObjectMapper();
    File file = new File(args.clustersFile);
    ClusterData clustersData = objectMapper.readValue(file, ClusterData.class);
    List<HostPort> hostAndPorts = clustersData.data.get(args.cluster);
    if (hostAndPorts == null) {
      LOG.error("Cluster {} not found in data file {}", args.cluster, args.clustersFile);
      return null;
    }
    Collections.shuffle(hostAndPorts);
    return hostAndPorts.get(0);
  }
}
