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

package com.facebook.giraph.hive.impl.input.benchmark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;

import com.facebook.giraph.hive.impl.common.HiveMetastores;
import com.facebook.giraph.hive.input.HiveApiInputFormat;
import com.facebook.giraph.hive.input.HiveInputDescription;
import com.google.common.collect.Lists;
import com.sampullara.cli.Args;

import java.io.IOException;
import java.util.List;

/**
 * Benchmark for input reading
 */
public class InputBenchmark {
  /** Logger */
  private static final Logger LOG = Logger.getLogger(InputBenchmark.class);

  /** How often to print info */
  private static final int PRINT_EVERY = 5;

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
  public static void main(String[] args)
    throws TTransportException, IOException, InterruptedException {
    HiveInputDescription input = new HiveInputDescription();
    BenchmarkArgs parsedArgs = handleCommandLine(args, input);
    if (parsedArgs == null) {
      return;
    }

    HiveConf hiveConf = new HiveConf(InputBenchmark.class);
    ThriftHiveMetastore.Iface client = HiveMetastores.create(
        parsedArgs.getHiveHost(), parsedArgs.getHivePort());

    System.err.println("Initialize profile with input data");
    try {
      HiveApiInputFormat.initProfile(hiveConf, input,
          HiveApiInputFormat.DEFAULT_PROFILE_ID, client);
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      System.err.println("Error initializing HiveInput: " + e);
      return;
    }

    MetricsObserver observer = new MetricsObserver();
    HiveApiInputFormat inputFormat = new HiveApiInputFormat();
    inputFormat.setObserver(observer);
    readAll(hiveConf, inputFormat);
  }

  /**
   * Read all data from an InputFormat
   *
   * @param conf Configuration
   * @param inputFormat InputFormat
   * @throws IOException I/O errors
   * @throws InterruptedException thread errors
   */
  private static void readAll(Configuration conf, InputFormat inputFormat)
    throws IOException, InterruptedException {
    JobID jobID = new JobID();
    JobContext jobContext = new JobContext(conf, jobID);
    List<RecordReader> readers = Lists.newArrayList();
    List<TaskAttemptContext> taskContexts = Lists.newArrayList();

    List<InputSplit> splits =  inputFormat.getSplits(jobContext);
    System.err.println("getSplits returned " + splits.size() + " splits");

    for (int i = 0; i < splits.size(); ++i) {
      InputSplit split = splits.get(i);
      TaskAttemptID taskID = new TaskAttemptID();
      TaskAttemptContext taskContext = new TaskAttemptContext(conf, taskID);
      taskContexts.add(taskContext);
      if (i % PRINT_EVERY == 0) {
        System.err.println("createRecordReader " + i + " of " +
            splits.size());
      }
      RecordReader reader = inputFormat.createRecordReader(split, taskContext);
      readers.add(reader);
    }

    for (int i = 0; i < splits.size(); ++i) {
      InputSplit split = splits.get(i);
      TaskAttemptContext taskContext = taskContexts.get(i);
      RecordReader reader = readers.get(i);
      if (i % PRINT_EVERY == 0) {
        System.err.println("initialize RecordReader " + i + " of " +
            splits.size());
      }
      reader.initialize(split, taskContext);
    }

    for (int i = 0; i < readers.size(); ++i) {
      RecordReader reader = readers.get(i);
      if (i % PRINT_EVERY == 0) {
        System.err.println("readFully from RecordReader " + i + " of " +
            splits.size());
      }
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
  private static void readFully(RecordReader reader)
    throws IOException, InterruptedException {
    float lastProgress = 0.0f;
    while (reader.nextKeyValue()) {
      float progress = reader.getProgress();
      if (progress - lastProgress >= 0.1) {
        LOG.info("Progress so far: " + progress);
      }
    }
  }

  /**
   * Parse command line, create hive input
   *
   * @param args Command line arguments
   * @param input Hive input to write to
   * @return Parsed arguments
   */
  private static BenchmarkArgs handleCommandLine(String[] args,
                                                 HiveInputDescription input) {
    BenchmarkArgs parsedArgs = new BenchmarkArgs();
    try {
      Args.parse(parsedArgs, args);
    } catch (IllegalArgumentException e) {
      System.err.println("ERROR: " + e);
      Args.usage(parsedArgs);
      return null;
    }

    input.setDbName(parsedArgs.getDatabase());
    input.setTableName(parsedArgs.getTable());
    input.setPartitionFilter(parsedArgs.getPartitionFilter());

    return parsedArgs;
  }
}
