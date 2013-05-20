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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.facebook.hiveio.common.HiveMetastores;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.testing.LocalHiveServer;

import java.io.IOException;
import java.util.Iterator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class InputTest {
  private LocalHiveServer hiveServer = new LocalHiveServer("hiveio-test");

  @BeforeMethod
  public void before() throws Exception {
    hiveServer.init();
    HiveMetastores.setTestClient(hiveServer.getClient());
  }

  @Test
  public void testInput() throws Exception
  {
    String tableName = "test1";

    String rows[] = {
      "1\t1.1",
      "2\t2.2",
    };
    hiveServer.createTable("CREATE TABLE " + tableName +
        " (i1 INT, d1 DOUBLE) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");
    hiveServer.loadData(tableName, rows);

    HiveInputDescription inputDesc = new HiveInputDescription();
    inputDesc.setTableName(tableName);

    verifyData(inputDesc);
  }

  @Test
  public void testInputWithPartitions() throws Exception
  {
    String tableName = "test1";
    String partition = "ds='foobar'";

    String rows[] = {
      "1\t1.1",
      "2\t2.2",
    };
    hiveServer.createTable("CREATE TABLE " + tableName +
        " (i1 INT, d1 DOUBLE) " +
        " PARTITIONED BY (ds STRING) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");
    hiveServer.loadData(tableName, partition, rows);

    HiveInputDescription inputDesc = new HiveInputDescription();
    inputDesc.setTableName(tableName);
    inputDesc.setPartitionFilter(partition);

    verifyData(inputDesc);
  }

  private void verifyData(HiveInputDescription inputDesc)
      throws IOException, InterruptedException {
    Iterator<HiveReadableRecord> records = HiveInput.readTable(inputDesc).iterator();

    assertTrue(records.hasNext());

    HiveReadableRecord record = records.next();
    assertEquals(Long.class, record.get(0).getClass());
    assertEquals(Double.class, record.get(1).getClass());
    assertEquals(1, record.getLong(0));
    assertEquals(1.1, record.getDouble(1));

    assertTrue(records.hasNext());
    record = records.next();
    assertEquals(2, record.getLong(0));
    assertEquals(2.2, record.getDouble(1));

    assertFalse(records.hasNext());
  }
  @Test
  public void testInputWithPartitions2() throws Exception {
	  String tableName = "test1";
    String partition = "ds='foo', ds2='bar'";
    String partition2 = "ds='foo', ds2='bar2'";

	  String rows[] = {
			  "1\t1.1",
			  "2\t2.2",
	  };
	  hiveServer.createTable("CREATE TABLE " + tableName +
			  " (i1 INT, d1 DOUBLE) " +
			  " PARTITIONED BY (ds STRING, ds2 STRING) " +
			  " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");
	  hiveServer.loadData(tableName, partition, rows);
    hiveServer.loadData(tableName, partition2, rows);

	  HiveInputDescription inputDesc = new HiveInputDescription();
	  inputDesc.setDbName("default");
	  inputDesc.setTableName(tableName);
	  inputDesc.addPartitionFilter(partition.split(",")[0]);
    inputDesc.addPartitionFilter(partition.split(",")[1]);
    inputDesc.addPartitionFilter(partition2.split(",")[0]);
    inputDesc.addPartitionFilter(partition2.split(",")[1]);

    Iterator<HiveReadableRecord> records = HiveInput.readTable(inputDesc).iterator();
    assertTrue(records.hasNext());

    HiveReadableRecord record = records.next();
    assertEquals(Long.class, record.get(0).getClass());
    assertEquals(Double.class, record.get(1).getClass());
    assertEquals(1, record.getLong(0));
    assertEquals(1.1, record.getDouble(1));

    assertTrue(records.hasNext());
    record = records.next();
    assertEquals(2, record.getLong(0));
    assertEquals(2.2, record.getDouble(1));

    assertTrue(records.hasNext());
    record = records.next();
    assertEquals(1, record.getLong(0));
    assertEquals(1.1, record.getDouble(1));

    assertTrue(records.hasNext());
    record = records.next();
    assertEquals(2, record.getLong(0));
    assertEquals(2.2, record.getDouble(1));
  }

}
