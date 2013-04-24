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
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.facebook.hiveio.schema.HiveTableSchemas;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.command.Help;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

import static com.facebook.hiveio.input.HiveApiInputFormat.DEFAULT_PROFILE_ID;

class Tailer {
  private static final Logger LOG = LoggerFactory.getLogger(Tailer.class);

  public void run(TailerCmd opts) throws Exception {
    HadoopNative.requireHadoopNative();

    HostPort metastoreHostPort = getHostPort(opts);
    if (metastoreHostPort == null) {
      return;
    }

    LOG.info("Creating Hive client for Metastore at {}", metastoreHostPort);
    ThriftHiveMetastore.Iface client = HiveMetastores.create(metastoreHostPort.host,
        metastoreHostPort.port);

    HiveInputDescription inputDesc = new HiveInputDescription();
    inputDesc.setDbName(opts.tableOpts.database);
    inputDesc.setTableName(opts.tableOpts.table);
    inputDesc.setPartitionFilter(opts.tableOpts.partitionFilter);
    if (opts.requestNumSplits == 0) {
      opts.requestNumSplits = opts.threads * opts.requestSplitsPerThread;
    }
    inputDesc.setNumSplits(opts.requestNumSplits);

    HiveStats hiveStats = HiveUtils.statsOf(client, inputDesc);
    LOG.info("{}", hiveStats);

    HiveConf hiveConf = new HiveConf(Tailer.class);

    LOG.info("Setting up input using {}", inputDesc);
    HiveApiInputFormat.setProfileInputDesc(hiveConf, inputDesc,
        DEFAULT_PROFILE_ID);

    HiveApiInputFormat hapi = new HiveApiInputFormat();
    hapi.setMyProfileId(DEFAULT_PROFILE_ID);

    List<InputSplit> splits = hapi.getSplits(hiveConf, client);
    LOG.info("Have {} splits to read", splits.size());

    HiveTableName hiveTableName = new HiveTableName(opts.tableOpts.database, opts.tableOpts.table);
    HiveTableSchema schema = HiveTableSchemas.lookup(client, hiveTableName);

    Stats stats = Stats.get(hiveStats);
    Context context = new Context(hapi, hiveConf, schema, hiveStats, opts, stats);
    long startNanos = System.nanoTime();

    if (opts.threads == 1) {
      context.splitsQueue = Queues.newArrayDeque(splits);
      readSplits(context);
    } else {
      context.splitsQueue = Queues.newConcurrentLinkedQueue(splits);
      multiThreaded(context, opts.threads);
    }

    long timeNanos = System.nanoTime() - startNanos;
    if (context.opts.appendStatsTo != null) {
      OutputStream out = new FileOutputStream(context.opts.appendStatsTo, true);
      stats.printEndBenchmark(context, timeNanos, out);
    }

    System.err.println("Finished.");
    if (opts.metricsOpts.stderrEnabled()) {
      opts.metricsOpts.dumpMetricsToStderr();
    }
  }

  private static void multiThreaded(final Context context, int numThreads)
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

  private static void readSplits(Context context)
  {
    while (context.hasMoreSplitsToRead()) {
      InputSplit split = context.splitsQueue.poll();
      try {
        readSplit(split, context);
      } catch (Exception e) {
        LOG.error("Failed to read split {}", split, e);
      }
    }
    context.perThread.get().flushBuffer();
  }

  private static void readSplit(InputSplit split, Context context)
      throws IOException, InterruptedException
  {
    TaskAttemptID taskId = new TaskAttemptID();
    TaskAttemptContext taskContext = new TaskAttemptContext(context.hiveConf, taskId);
    RecordReader<WritableComparable, HiveReadableRecord> recordReader;
    recordReader = context.hiveApiInputFormat.createRecordReader(split, taskContext);
    recordReader.initialize(split, taskContext);

    int rowsParsed = 0;
    while (recordReader.nextKeyValue() && !context.limitReached()) {
      HiveReadableRecord record = recordReader.getCurrentValue();
      if (context.opts.parseOnly) {
        parseRecord(record, context.schema.numColumns());
      } else {
        context.opts.recordPrinter.printRecord(record,
            context.schema.numColumns(), context);
      }
      ++rowsParsed;
      if (context.rowsParsed.incrementAndGet() > context.opts.limit) {
        break;
      }
      if (rowsParsed % context.opts.metricsOpts.updateRows == 0) {
        context.stats.addRows(context.opts.metricsOpts.updateRows);
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

  private static HostPort getHostPort(TailerCmd args) throws IOException {
    if (args.clustersFile == null) {
      return new HostPort(args.metastoreOpts.hiveHost, args.metastoreOpts.hivePort);
    }
    return clusterFromConfig(args);
  }

  private static HostPort clusterFromConfig(TailerCmd args) throws IOException {
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
