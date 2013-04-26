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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.options.BaseCmd;
import com.facebook.hiveio.record.HiveRecordFactory;
import com.facebook.hiveio.record.HiveWritableRecord;
import io.airlift.command.Command;

import javax.inject.Inject;

@Command(name = "output")
public class OutputCmd extends BaseCmd {
  public static final int NUM_COLUMNS = 4;

  private static final Logger LOG = LoggerFactory.getLogger(OutputCmd.class);

  @Inject OutputArgs args = new OutputArgs();

  /**
   * Before running this:

   CREATE TABLE IF NOT EXISTS hiveio_output_test(
     i1 BIGINT,
     d2 DOUBLE,
     b3 BOOLEAN,
     s4 STRING
   ) TBLPROPERTIES ('RETENTION_PLATINUM'='90')

   CREATE TABLE IF NOT EXISTS hiveio_output_test_partitioned (
     i1 BIGINT,
     d2 DOUBLE,
     b3 BOOLEAN,
     s4 STRING
   )
   PARTITIONED BY (ds STRING)
   TBLPROPERTIES ('RETENTION_PLATINUM'='90')

   * @throws Exception
   */
  @Override
  public void execute() throws Exception {
    args.table.process();

    Context context = new Context();
    context.conf.setInt("mapred.task.partition", 1);

    HiveOutputDescription outputDesc = new HiveOutputDescription();
    outputDesc.setDbName(args.table.database);
    outputDesc.setTableName(args.table.table);
    outputDesc.getMetastoreDesc().setHost(args.metastore.host);
    outputDesc.getMetastoreDesc().setPort(args.metastore.port);
    if (args.table.partitioned) {
      outputDesc.putPartitionValue("ds", "2013-04-01");
    }

    context.outputFormat = new HiveApiOutputFormat();
    context.outputFormat.init(context.conf, outputDesc);

    PerThread threadLocal = context.perThread.get();
    context.outputFormat.checkOutputSpecs(threadLocal.jobContext());

    HiveApiOutputCommitter outputCommitter =
        context.outputFormat.getOutputCommitter(threadLocal.taskContext());

    outputCommitter.setupJob(threadLocal.jobContext());

    if (args.multiThread.isMultiThreaded()) {
      multiThreaded(context);
    } else {
      singleThreaded(context);
    }

    outputCommitter.commitJob(threadLocal.jobContext());
  }

  private void singleThreaded(Context context) throws Exception {
    write(context);
  }

  private void multiThreaded(final Context context)
      throws InterruptedException {
    Thread[] threads = new Thread[args.multiThread.threads];
    for (int i = 0; i < args.multiThread.threads; ++i) {
      final int threadId = i + 1;
      threads[i] = new Thread(new Runnable() {
        @Override public void run() {
          try {
            write(context);
          } catch (Exception e) {
            LOG.error("Thread {} failed to write", Thread.currentThread().getId(), e);
          }
        }
      });
      threads[i].setName("writer-" + threadId);
      threads[i].start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
  }

  public void write(Context context) throws Exception
  {
    PerThread threadLocal = context.perThread.get();

    HiveApiOutputCommitter outputCommitter =
        context.outputFormat.getOutputCommitter(threadLocal.taskContext());

    outputCommitter.setupTask(threadLocal.taskContext());

    RecordWriter<WritableComparable, HiveWritableRecord> recordWriter =
        context.outputFormat.getRecordWriter(threadLocal.taskContext());

    HiveWritableRecord record = HiveRecordFactory.newWritableRecord(NUM_COLUMNS);

    record.set(0, 11L);     // TODO: allow type promotions: see https://github.com/facebook/hive-io-experimental/issues/15
    record.set(1, 22.22);
    record.set(2, true);
    record.set(3, "foo");
    recordWriter.write(NullWritable.get(), record);

    record.set(0, 33L);
    record.set(1, 44.44);
    record.set(2, false);
    record.set(3, "bar");
    recordWriter.write(NullWritable.get(), record);

    recordWriter.close(threadLocal.taskContext());

    if (outputCommitter.needsTaskCommit(threadLocal.taskContext())) {
      outputCommitter.commitTask(threadLocal.taskContext());
    }
  }
}
