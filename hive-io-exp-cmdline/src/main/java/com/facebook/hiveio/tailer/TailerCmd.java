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
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.HadoopNative;
import com.facebook.hiveio.common.HiveMetastores;
import com.facebook.hiveio.common.HiveStats;
import com.facebook.hiveio.common.HiveTableDesc;
import com.facebook.hiveio.common.HiveUtils;
import com.facebook.hiveio.input.HiveApiInputFormat;
import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.options.BaseCmd;
import com.facebook.hiveio.common.HostPort;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.facebook.hiveio.schema.HiveTableSchemas;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.command.Command;

import javax.inject.Inject;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static com.facebook.hiveio.input.HiveApiInputFormat.DEFAULT_PROFILE_ID;

/**
 * hivetail command
 */
@Command(name = "tail", description = "Dump a Hive table")
public class TailerCmd extends BaseCmd
{
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(TailerCmd.class);

  /** command line args */
  @Inject private TailerArgs args = new TailerArgs();

  /** record printer */
  private RecordPrinter recordPrinter;
  /** row parser */
  private RowParser rowParser;

  /** set record printer to use */
  public void chooseRecordPrinter()
  {
    if (args.recordBufferFlush > 1) {
      recordPrinter = new RecordPrinter.Buffered();
    } else {
      recordPrinter = new RecordPrinter.Default();
    }
  }

  /**
   * Set row parser to use
   *
   * @param schema table schema
   */
  public void chooseRowParser(HiveTableSchema schema)
    throws ClassNotFoundException, InstantiationException, IllegalAccessException
  {
    if (args.parser.beanParser) {
      Class<?> klass = Class.forName(args.parser.rowClassName);
      rowParser = new RowParser.Bean(schema, klass);
    } else {
      rowParser = new RowParser.Default();
    }
  }

  @Override
  public void execute() throws Exception
  {
    HadoopNative.requireHadoopNative();

    args.process();
    chooseRecordPrinter();

    HostPort metastoreHostPort = getMetastoreHostPort();
    if (metastoreHostPort == null) {
      return;
    }

    LOG.info("Creating Hive client for Metastore at {}", metastoreHostPort);
    ThriftHiveMetastore.Iface client = HiveMetastores.create(
        metastoreHostPort.host, metastoreHostPort.port);

    HiveInputDescription inputDesc = initInput(metastoreHostPort);

    HiveStats hiveStats = HiveUtils.statsOf(client, inputDesc);
    LOG.info("{}", hiveStats);

    HiveConf hiveConf = HiveUtils.newHiveConf(TailerCmd.class);
    args.inputTable.process(hiveConf);

    LOG.info("Setting up input using {}", inputDesc);
    HiveApiInputFormat.setProfileInputDesc(hiveConf, inputDesc,
        DEFAULT_PROFILE_ID);

    HiveApiInputFormat hapi = new HiveApiInputFormat();
    hapi.setMyProfileId(DEFAULT_PROFILE_ID);

    List<InputSplit> splits = hapi.getSplits(new JobContext(hiveConf, new JobID()));
    LOG.info("Have {} splits to read", splits.size());

    HiveTableDesc hiveTableDesc = new HiveTableDesc(args.inputTable.database,
        args.inputTable.table);
    HiveTableSchema schema = HiveTableSchemas.lookup(client, hiveConf,
        hiveTableDesc);
    chooseRowParser(schema);

    Stats stats = Stats.create(hiveStats);
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
      try {
        stats.printEndBenchmark(context, args, timeNanos, out);
      } finally {
        out.close();
      }
    }

    System.err.println("Finished.");
    if (args.metricsOpts.stderrEnabled()) {
      args.metricsOpts.dumpMetricsToStderr();
    }
  }

  /**
   * Initialize hive input
   *
   * @param metastoreHostPort metastore location info
   * @return HiveInputDescription
   */
  private HiveInputDescription initInput(HostPort metastoreHostPort) {
    HiveInputDescription inputDesc = new HiveInputDescription();
    inputDesc.getTableDesc().setDatabaseName(args.inputTable.database);
    inputDesc.getTableDesc().setTableName(args.inputTable.table);
    inputDesc.setPartitionFilter(args.inputTable.partitionFilter);
    args.splits.compute(args.multiThread.threads);
    inputDesc.setNumSplits(args.splits.requestNumSplits);
    inputDesc.getMetastoreDesc().setHost(metastoreHostPort.host);
    inputDesc.getMetastoreDesc().setPort(metastoreHostPort.port);
    return inputDesc;
  }

  /**
   * Multi threaded execution
   *
   * @param context Context
   * @param numThreads number of threads
   */
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

  /**
   * Read input splits
   *
   * @param context Context
   */
  private void readSplits(Context context)
  {
    while (context.hasMoreSplitsToRead(args.limit)) {
      InputSplit split = context.splitsQueue.poll();
      try {
        readSplit(split, context);
        // CHECKSTYLE: stop IllegalCatch
      } catch (Exception e) {
        // CHECKSTYLE: resume IllegalCatch
        LOG.error("Failed to read split {}", split, e);
      }
    }
    context.perThread.get().flushBuffer();
  }

  /**
   * Read input split
   *
   * @param split InputSplit
   * @param context Context
   * @throws IOException
   * @throws InterruptedException
   */
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
      if (args.parser.parseOnly) {
        rowParser.parse(record);
      } else {
        recordPrinter.printRecord(record, context.schema.numColumns(), context, args);
      }
      ++rowsParsed;
      if (context.rowsParsed.incrementAndGet() >= args.limit) {
        break;
      }
      if (rowsParsed % args.metricsOpts.updateRows == 0) {
        context.stats.addRows(args.metricsOpts.updateRows);
        rowsParsed = 0;
      }
    }
    context.stats.addRows(rowsParsed);
  }

  /**
   * Get metastore HostPort
   *
   * @return Metastore HostPort
   * @throws IOException
   */
  private HostPort getMetastoreHostPort() throws IOException {
    HostPort metastoreInfo;
    if (args.namespace.hasPath()) {
      metastoreInfo = args.namespace.readMetastoreInfo();
    } else {
      metastoreInfo = new HostPort(args.metastore.host, args.metastore.port);
    }
    return metastoreInfo;
  }
}
