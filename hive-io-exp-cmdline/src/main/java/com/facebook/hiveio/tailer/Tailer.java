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
import org.apache.log4j.Logger;

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
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.hiveio.input.HiveApiInputFormat.DEFAULT_PROFILE_ID;

class Tailer {
  private static final Logger LOG = Logger.getLogger(Tailer.class);

  public void initMetrics(int metricsPrintPeriodSecs) {
    if (metricsPrintPeriodSecs > 0) {
      Stats.metricsReporter().start(metricsPrintPeriodSecs, TimeUnit.SECONDS);
    }
  }

  public void run(TailerCmd opts) throws Exception {
    initMetrics(opts.metricsPrintPeriodSecs);

    HadoopNative.requireHadoopNative();

    HostPort metastoreHostPort = getHostPort(opts);
    if (metastoreHostPort == null) {
      return;
    }

    ThriftHiveMetastore.Iface client = HiveMetastores.create(metastoreHostPort.host,
        metastoreHostPort.port);

    HiveInputDescription inputDesc = new HiveInputDescription();
    inputDesc.setDbName(opts.database);
    inputDesc.setTableName(opts.table);
    inputDesc.setPartitionFilter(opts.partitionFilter);
    if (opts.requestNumSplits == 0) {
      opts.requestNumSplits = opts.threads * opts.requestSplitsPerThread;
    }
    inputDesc.setNumSplits(opts.requestNumSplits);

    HiveStats hiveStats = HiveUtils.statsOf(client, inputDesc);
    LOG.info(hiveStats);

    HiveConf hiveConf = new HiveConf(Tailer.class);
    HiveApiInputFormat
        .setProfileInputDesc(hiveConf, inputDesc, DEFAULT_PROFILE_ID);

    HiveApiInputFormat hapi = new HiveApiInputFormat();
    hapi.setMyProfileId(DEFAULT_PROFILE_ID);

    List<InputSplit> splits = hapi.getSplits(hiveConf, client);
    LOG.info("Have " + splits.size() + " splits to read");

    HiveTableName hiveTableName = new HiveTableName(opts.database, opts.table);
    HiveTableSchema schema = HiveTableSchemas.lookup(client, hiveTableName);

    Stats stats = new Stats();
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
    stats.printEnd(hiveStats, timeNanos);
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
        LOG.error("Failed to read split " + split, e);
      }
    }
  }

  private static void readSplit(InputSplit split, Context context)
      throws IOException, InterruptedException
  {
    TaskAttemptID taskId = new TaskAttemptID();
    TaskAttemptContext taskContext = new TaskAttemptContext(context.hiveConf, taskId);
    RecordReader<WritableComparable, HiveReadableRecord> recordReader;
    recordReader = context.hiveApiInputFormat.createRecordReader(split, taskContext);
    recordReader.initialize(split, taskContext);
    String[] values = new String[context.schema.numColumns()];

    int rowsParsed = 0;
    while (recordReader.nextKeyValue() && !context.limitReached()) {
      HiveReadableRecord record = recordReader.getCurrentValue();
      printRecord(record, context.schema.numColumns(), context, values);
      ++rowsParsed;
      if (context.rowsParsed.incrementAndGet() > context.opts.limit) {
        break;
      }
      if (rowsParsed % context.opts.metricsUpdatePeriodRows == 0) {
        context.stats.addRows(context.hiveStats, context.opts.metricsUpdatePeriodRows);
        rowsParsed = 0;
      }
    }
    context.stats.addRows(context.hiveStats, rowsParsed);
  }

  private static void printRecord(HiveReadableRecord record, int numColumns,
      Context context, String[] values)
  {
    StringBuilder sb = context.threadContext.get().stringBuilder;
    sb.setLength(0);
    for (int index = 0; index < numColumns; ++index) {
      if (index > 0) {
        sb.append(context.opts.separator);
      }
      sb.append(String.valueOf(record.get(index)));
    }
    System.out.println(sb.toString());
  }

  private static HostPort getHostPort(TailerCmd args) throws IOException {
    HostPort metastoreHostPort = new HostPort();
    metastoreHostPort.port = args.metastorePort;
    if (args.metastoreHost != null) {
      metastoreHostPort.host = args.metastoreHost;
    }
    if (metastoreHostPort.host == null && args.cluster != null) {
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
        LOG.error("Cluster " + args.cluster +
            " not found in data file " + args.clustersFile);
        return null;
      }
      Collections.shuffle(hostAndPorts);
      metastoreHostPort = hostAndPorts.get(0);
    }
    return metastoreHostPort;
  }
}
