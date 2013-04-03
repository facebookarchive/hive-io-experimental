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
package com.facebook.giraph.hive.tailer;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import com.facebook.giraph.hive.common.HadoopNative;
import com.facebook.giraph.hive.common.HiveMetastores;
import com.facebook.giraph.hive.common.HiveStats;
import com.facebook.giraph.hive.common.HiveTableName;
import com.facebook.giraph.hive.common.HiveUtils;
import com.facebook.giraph.hive.input.HiveApiInputFormat;
import com.facebook.giraph.hive.input.HiveInputDescription;
import com.facebook.giraph.hive.record.HiveReadableRecord;
import com.facebook.giraph.hive.schema.HiveTableSchema;
import com.facebook.giraph.hive.schema.HiveTableSchemas;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Uninterruptibles;
import com.sampullara.cli.Args;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

import static com.facebook.giraph.hive.input.HiveApiInputFormat.DEFAULT_PROFILE_ID;
import static com.facebook.giraph.hive.input.HiveApiInputFormat.LOG;

public class Tailer {
  private static class Context {
    public Queue<InputSplit> splitsQueue;
    public HiveApiInputFormat hiveApiInputFormat;
    public HiveConf hiveConf;
    public HiveTableSchema schema;
    public HiveStats hiveStats;
    public Opts opts;
    public Stats stats;

    private Context(HiveApiInputFormat hiveApiInputFormat, HiveConf hiveConf,
        HiveTableSchema schema, HiveStats hiveStats, Opts opts, Stats stats) {
      this.hiveApiInputFormat = hiveApiInputFormat;
      this.hiveConf = hiveConf;
      this.schema = schema;
      this.hiveStats = hiveStats;
      this.opts = opts;
      this.stats = stats;
    }
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    try {
      Args.parse(opts, args);
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      Args.usage(opts);
      return;
    }
    if (opts.help) {
      Args.usage(opts);
      return;
    }
    opts.process();

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
      opts.requestNumSplits = opts.threads * opts.splitsPerThread;
    }
    inputDesc.setNumSplits(opts.requestNumSplits);

    HiveStats hiveStats = HiveUtils.statsOf(client, inputDesc);
    System.err.println(hiveStats);

    HiveConf hiveConf = new HiveConf(Tailer.class);
    HiveApiInputFormat.setProfileInputDesc(hiveConf, inputDesc, DEFAULT_PROFILE_ID);

    HiveApiInputFormat hapi = new HiveApiInputFormat();
    hapi.setMyProfileId(DEFAULT_PROFILE_ID);

    List<InputSplit> splits = hapi.getSplits(hiveConf, client);
    System.err.println("Have " + splits + " splits to read");

    HiveTableName hiveTableName = new HiveTableName(opts.database, opts.table);
    HiveTableSchema schema = HiveTableSchemas.lookup(client, hiveTableName);

    Stats stats = new Stats();
    Context context = new Context(hapi, hiveConf, schema, hiveStats, opts, stats);
    long startNanos = System.nanoTime();

    if (opts.threads == 1) {
      context.splitsQueue = Queues.newArrayDeque(splits);
      stats.numRows = readSplits(context);
    } else {
      context.splitsQueue = Queues.newConcurrentLinkedQueue(splits);
      stats.numRows = multiThreaded(context, opts.threads);
    }

    stats.timeNanos = System.nanoTime() - startNanos;
    stats.printEnd(hiveStats);
  }

  private static long multiThreaded(final Context context, int numThreads)
  {
    List<Future<Long>> futures = Lists.newArrayList();
    List<Thread> threads = Lists.newArrayList();
    for (int i = 0; i < numThreads; ++i) {
      RunnableFuture<Long> future = new FutureTask(new Callable<Long>() {
        @Override public Long call() throws Exception {
          return readSplits(context);
        }
      });

      Thread thread = new Thread(future);
      thread.setName("readSplit-" + i);
      thread.start();

      futures.add(future);
      threads.add(thread);
    }

    long numRows = 0;
    for (int i = 0; i < threads.size(); ++i) {
      Uninterruptibles.joinUninterruptibly(threads.get(i));
      try {
        numRows += Uninterruptibles.getUninterruptibly(futures.get(i));
      } catch (ExecutionException e) {
        LOG.error("Could not get future " + i + " numRows");
      }
    }
    return numRows;
  }

  private static long readSplits(Context context)
  {
    long totalRows = 0;
    while (!context.splitsQueue.isEmpty()) {
      InputSplit split = context.splitsQueue.poll();
      try {
        totalRows += readSplit(split, context);
      } catch (Exception e) {
        System.err.println("Failed to read split " + split);
        e.printStackTrace();
      }
    }
    return totalRows;
  }

  private static long readSplit(InputSplit split, Context context)
      throws IOException, InterruptedException
  {
    long numRows = 0;
    TaskAttemptID taskId = new TaskAttemptID();
    TaskAttemptContext taskContext = new TaskAttemptContext(context.hiveConf, taskId);
    RecordReader<WritableComparable, HiveReadableRecord> recordReader;
    recordReader = context.hiveApiInputFormat.createRecordReader(split, taskContext);
    recordReader.initialize(split, taskContext);
    String[] values = new String[context.schema.numColumns()];
    while (recordReader.nextKeyValue()) {
      HiveReadableRecord record = recordReader.getCurrentValue();
      printRecord(record, context.schema.numColumns(), context.opts, values);
      ++numRows;
      if (numRows % Opts.METRICS_ROWS_UPDATE == 0) {
        context.stats.addRows(context.hiveStats, Opts.METRICS_ROWS_UPDATE);
      }
    }
    return numRows;
  }

  private static void printRecord(HiveReadableRecord record, int numColumns,
      Opts opts, String[] values)
  {
    for (int index = 0; index < numColumns; ++index) {
      values[index] = String.valueOf(record.get(index));
    }
    System.out.println(opts.joiner.join(values));
  }

  private static HostPort getHostPort(Opts opts) throws IOException {
    HostPort metastoreHostPort = new HostPort();
    metastoreHostPort.port = opts.metastorePort;
    if (opts.metastoreHost != null) {
      metastoreHostPort.host = opts.metastoreHost;
    }
    if (metastoreHostPort.host == null && opts.cluster != null) {
      ClusterData clustersData = new ClusterData();
      if (opts.clustersFile == null) {
        System.err.println("ERROR: Cluster file not given");
        Args.usage(opts);
        return null;
      }
      if (opts.clustersFile != null) {
        ObjectMapper objectMapper = new ObjectMapper();
        File file = new File(opts.clustersFile);
        clustersData = objectMapper.readValue(file, ClusterData.class);
      }
      List<HostPort> hostAndPorts = clustersData.data.get(opts.cluster);
      if (hostAndPorts == null) {
        System.err.println("Cluster " + opts.cluster +
            " not found in data file " + opts.clustersFile);
        return null;
      }
      Collections.shuffle(hostAndPorts);
      metastoreHostPort = hostAndPorts.get(0);
    }
    return metastoreHostPort;
  }
}
