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
import com.facebook.giraph.hive.common.HiveTableName;
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
import java.util.concurrent.TimeUnit;

import static com.facebook.giraph.hive.input.HiveApiInputFormat.DEFAULT_PROFILE_ID;
import static com.facebook.giraph.hive.input.HiveApiInputFormat.LOG;

public class Tailer {
  public static final int DEFAULT_METASTORE_PORT = 9083;

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

    HiveConf hiveConf = new HiveConf(Tailer.class);
    HiveApiInputFormat.setProfileInputDesc(hiveConf, inputDesc, DEFAULT_PROFILE_ID);

    HiveApiInputFormat hapi = new HiveApiInputFormat();
    hapi.setMyProfileId(DEFAULT_PROFILE_ID);

    List<InputSplit> splits = hapi.getSplits(hiveConf, client);

    HiveTableName hiveTableName = new HiveTableName(opts.database, opts.table);
    HiveTableSchema schema = HiveTableSchemas.lookup(client, hiveTableName);

    Queue<InputSplit> splitsQueue;
    long numRows;
    long startNanos = System.nanoTime();
    if (opts.threads == 1) {
      splitsQueue = Queues.newArrayDeque(splits);
      numRows = readSplits(splitsQueue, hapi, hiveConf, schema, opts);
    } else {
      splitsQueue = Queues.newConcurrentLinkedQueue(splits);
      numRows = multiThreaded(opts.threads, splitsQueue, hapi, hiveConf, schema, opts);
    }
    long diffNanos = System.nanoTime() - startNanos;
    System.err.println("Processed " + numRows + " rows in " +
        TimeUnit.NANOSECONDS.toMillis(diffNanos) + " msec");
  }

  private static long multiThreaded(int numThreads, final Queue<InputSplit> splitsQueue,
      final HiveApiInputFormat hapi, final HiveConf hiveConf,
      final HiveTableSchema schema, final Opts opts)
  {
    long numRows = 0;
    List<Thread> threads = Lists.newArrayList();
    List<Future<Long>> futures = Lists.newArrayList();
    for (int i = 0; i < numThreads; ++i) {
      RunnableFuture<Long> future = new FutureTask(new Callable<Long>() {
        @Override public Long call() throws Exception {
          return readSplits(splitsQueue, hapi, hiveConf, schema, opts);
        }
      });
      futures.add(future);
      Thread thread = new Thread(future);
      thread.setName("readSplit-" + i);
      thread.start();
    }
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

  private static long readSplits(Queue<InputSplit> splitsQueue, HiveApiInputFormat hapi,
      HiveConf hiveConf, HiveTableSchema schema, Opts opts)
  {
    long numRows = 0;
    while (!splitsQueue.isEmpty()) {
      InputSplit split = splitsQueue.poll();
      try {
        numRows += readSplit(split, hapi, hiveConf, schema, opts);
      } catch (Exception e) {
        System.err.println("Failed to read split " + split);
        e.printStackTrace();
      }
    }
    return numRows;
  }

  private static long readSplit(InputSplit split, HiveApiInputFormat hapi,
      HiveConf hiveConf, HiveTableSchema schema, Opts opts)
      throws IOException, InterruptedException
  {
    long numRows = 0;
    TaskAttemptID taskId = new TaskAttemptID();
    TaskAttemptContext taskContext = new TaskAttemptContext(hiveConf, taskId);
    RecordReader<WritableComparable, HiveReadableRecord> recordReader;
    recordReader = hapi.createRecordReader(split, taskContext);
    recordReader.initialize(split, taskContext);
    String[] values = new String[schema.numColumns()];
    while (recordReader.nextKeyValue()) {
      HiveReadableRecord record = recordReader.getCurrentValue();
      printRecord(record, schema, opts, values);
      ++numRows;
    }
    return numRows;
  }

  private static void printRecord(HiveReadableRecord record, HiveTableSchema schema,
      Opts opts, String[] values)
  {
    for (int index = 0; index < schema.numColumns(); ++index) {
      values[index] = record.get(index).toString();
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
      if (opts.clusterFile == null) {
        System.err.println("ERROR: Cluster file not given");
        Args.usage(opts);
        return null;
      }
      if (opts.clusterFile != null) {
        ObjectMapper objectMapper = new ObjectMapper();
        File file = new File(opts.clusterFile);
        clustersData = objectMapper.readValue(file, ClusterData.class);
      }
      List<HostPort> hostAndPorts = clustersData.data.get(opts.cluster);
      if (hostAndPorts == null) {
        System.err.println("Cluster " + opts.cluster +
            " not found in data file " + opts.clusterFile);
        return null;
      }
      Collections.shuffle(hostAndPorts);
      metastoreHostPort = hostAndPorts.get(0);
    }
    return metastoreHostPort;
  }
}
