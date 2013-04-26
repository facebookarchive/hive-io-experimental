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
package com.facebook.hiveio.input;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.facebook.hiveio.common.HiveMetastores;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.testing.LocalHiveServer;

import java.io.IOException;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class HiveApiInputFormatTest {
  private LocalHiveServer hiveServer = new LocalHiveServer("hiveio-test");

  @BeforeSuite
  public void beforeSuite() throws Exception {
    hiveServer.init();
    HiveMetastores.setTestClient(hiveServer.getClient());
  }

  @Test
  public void testInput() throws Exception {
    String tableName = "t1";
    initData(tableName);
    run1(tableName);
  }

  private void run1(String tableName) throws IOException, InterruptedException
  {
    HiveConf hiveConf = hiveServer.getHiveConf();

    HiveInputDescription hid = new HiveInputDescription();
    hid.setTableName(tableName);
    HiveApiInputFormat.setProfileInputDesc(hiveConf, hid,
        HiveApiInputFormat.DEFAULT_PROFILE_ID);

    HiveApiInputFormat haif = new HiveApiInputFormat();

    List<InputSplit> splits = haif.getSplits(hiveConf, hid, hiveServer.getClient());

    TaskAttemptContext taskContext = new TaskAttemptContext(hiveConf, new TaskAttemptID());
    InputSplit split = splits.get(0);
    RecordReader<WritableComparable, HiveReadableRecord> recordReader =
        haif.createRecordReader(split, taskContext);
    recordReader.initialize(split, taskContext);

    assertTrue(recordReader.nextKeyValue());

    HiveReadableRecord record = recordReader.getCurrentValue();
    assertEquals(Long.class, record.get(0).getClass());
    assertEquals(Double.class, record.get(1).getClass());
    assertEquals(1, record.getLong(0));
    assertEquals(1.1, record.getDouble(1));

    assertTrue(recordReader.nextKeyValue());
    record = recordReader.getCurrentValue();
    assertEquals(2, record.getLong(0));
    assertEquals(2.2, record.getDouble(1));

    assertFalse(recordReader.nextKeyValue());
  }

  private void initData(String tableName)
      throws IOException, org.apache.thrift.TException {
    String rows[] = {
      "1\t1.1",
      "2\t2.2",
    };
    hiveServer.createTable("CREATE TABLE " + tableName +
        " (i1 INT, d1 DOUBLE) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");
    hiveServer.loadData(tableName, rows);
  }
}
