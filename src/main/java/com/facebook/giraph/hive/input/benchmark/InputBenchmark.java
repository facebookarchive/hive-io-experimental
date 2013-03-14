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

package com.facebook.giraph.hive.input.benchmark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;

import com.facebook.giraph.hive.common.HiveMetastores;
import com.facebook.giraph.hive.input.HiveApiInputFormat;
import com.facebook.giraph.hive.input.HiveInputDescription;
import com.facebook.giraph.hive.record.HiveRecord;
import com.google.common.base.Optional;
import com.sampullara.cli.Args;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import com.yammer.metrics.reporting.ConsoleReporter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for input reading
 */
public class InputBenchmark {
  /** Logger */
  private static final Logger LOG = Logger.getLogger(InputBenchmark.class);

  /** Don't construct */
  protected InputBenchmark() { }

  /**
   * Main Entry point
   *
   * @param args Command line arguments
   * @throws TTransportException thrift errors
   * @throws IOException I/O errors
   * @throws InterruptedException thread errors
   */
  public static void main(String[] args) throws Exception
  {
    HadoopNative.requireHadoopNative();

    Optional<BenchmarkArgs> parsedArgs = handleCommandLine(args);
    if (!parsedArgs.isPresent()) {
      return;
    }

    Timer allTime = Metrics.newTimer(InputBenchmark.class, "all-time", TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS);
    TimerContext allTimerContext = allTime.time();
    run(parsedArgs.get());
    allTimerContext.stop();

    new ConsoleReporter(System.err).run();
  }

  /**
   * Parse command line, create hive input
   *
   * @param args Command line arguments
   * @return Parsed arguments
   */
  private static Optional<BenchmarkArgs> handleCommandLine(String[] args) {
    BenchmarkArgs parsedArgs = new BenchmarkArgs();
    try {
      Args.parse(parsedArgs, args);
    } catch (IllegalArgumentException e) {
      System.err.println("ERROR: " + e);
      Args.usage(parsedArgs);
      return Optional.absent();
    }
    if (parsedArgs.isHelp()) {
      Args.usage(parsedArgs);
      return Optional.absent();
    }

    return Optional.of(parsedArgs);
  }

  private static void run(BenchmarkArgs parsedArgs)
      throws TTransportException, IOException, InterruptedException {
    HiveInputDescription input = new HiveInputDescription();
    input.setDbName(parsedArgs.getDatabase());
    input.setTableName(parsedArgs.getTable());
    input.setPartitionFilter(parsedArgs.getPartitionFilter());

    HiveConf hiveConf = new HiveConf(InputBenchmark.class);
    ThriftHiveMetastore.Iface client = HiveMetastores.create(parsedArgs.getHiveHost(), parsedArgs.getHivePort());

    System.err.println("Initialize profile with input data");
    try {
      HiveApiInputFormat.initProfile(hiveConf, input, HiveApiInputFormat.DEFAULT_PROFILE_ID, client);
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      System.err.println("Error initializing HiveInput: " + e);
      return;
    }

    readAll(hiveConf, parsedArgs);
  }

  /**
   * Read all data from an InputFormat
   *
   * @param conf Configuration
   * @param inputFormat InputFormat
   * @throws IOException I/O errors
   * @throws InterruptedException thread errors
   */
  private static void readAll(Configuration conf, BenchmarkArgs args)
    throws IOException, InterruptedException {
    HiveApiInputFormat defaultInputFormat = new HiveApiInputFormat();
    defaultInputFormat.setObserver(new MetricsObserver("default", args.getRecordPrintPeriod()));

    JobID jobID = new JobID();
    JobContext jobContext = new JobContext(conf, jobID);

    List<InputSplit> splits =  defaultInputFormat.getSplits(jobContext);
    System.err.println("getSplits returned " + splits.size() + " splits");

    for (int i = 0; i < splits.size(); ++i) {
      InputSplit split = splits.get(i);
      TaskAttemptID taskID = new TaskAttemptID();
      TaskAttemptContext taskContext = new TaskAttemptContext(conf, taskID);
      if (i % args.getSplitPrintPeriod() == 0) {
        System.err.println("Handling split " + i + " of " + splits.size());
      }
      RecordReader<WritableComparable, HiveRecord> reader = defaultInputFormat.createRecordReader(split, taskContext);
      reader.initialize(split, taskContext);
      readFully(reader);
    }
  }

  /**
   * Read all records from a RecordReader
   *
   * @param reader RecordReader
   * @throws IOException I/O errors
   * @throws InterruptedException thread errors
   */
  private static void readFully(RecordReader<WritableComparable, HiveRecord> reader)
    throws IOException, InterruptedException {
    float lastProgress = 0.0f;
    while (reader.nextKeyValue()) {
      HiveRecord record = reader.getCurrentValue();
      record.get(0);
      record.get(1);
      record.get(2);
    }
  }
}
