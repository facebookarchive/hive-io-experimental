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

import org.apache.thrift.TException;
import org.testng.annotations.BeforeSuite;
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

  @BeforeSuite
  public void beforeSuite() throws Exception {
    hiveServer.init();
    HiveMetastores.setTestClient(hiveServer.getClient());
  }

  @Test
  private void testInput() throws IOException, InterruptedException, TException
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

    HiveInputDescription hid = new HiveInputDescription();
    hid.setTableName(tableName);

    Iterator<HiveReadableRecord> records = HiveInput.readTable(hid).iterator();

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
}
