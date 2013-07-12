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

import org.apache.thrift.TException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.facebook.hiveio.common.HiveIOTestBase;
import com.facebook.hiveio.common.HiveMetastores;
import com.facebook.hiveio.common.HiveTableDesc;
import com.facebook.hiveio.input.HiveInput;
import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.record.HiveRecordFactory;
import com.facebook.hiveio.record.HiveWritableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.facebook.hiveio.schema.HiveTableSchemas;
import com.facebook.hiveio.testing.LocalHiveServer;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class OutputTest extends HiveIOTestBase {
  private final LocalHiveServer hiveServer = new LocalHiveServer("hiveio-test");
  private final HiveTableDesc hiveTableDesc = new HiveTableDesc("default",
      OutputTest.class.getSimpleName());

  @BeforeMethod
  public void beforeSuite() throws Exception {
    hiveServer.init();
    HiveMetastores.setTestClient(hiveServer.getClient());
  }

  @Test
  public void testOutput() throws Exception
  {
    hiveServer.createTable("CREATE TABLE " + hiveTableDesc.getTableName() +
        " (i1 INT, d1 DOUBLE) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");

    HiveOutputDescription outputDesc = new HiveOutputDescription();
    outputDesc.setTableDesc(hiveTableDesc);

    HiveTableSchema schema = HiveTableSchemas.lookup(hiveServer.getClient(),
        null, hiveTableDesc);

    writeData(outputDesc, schema);

    HiveInputDescription inputDesc = new HiveInputDescription();
    inputDesc.setTableDesc(hiveTableDesc);

    verifyData(inputDesc);
  }

  @Test
  public void testOutputWithPartitions() throws Exception
  {
    hiveServer.createTable("CREATE TABLE " + hiveTableDesc.getTableName() +
        " (i1 INT, d1 DOUBLE) " +
        " PARTITIONED BY (ds STRING) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");

    HiveOutputDescription outputDesc = new HiveOutputDescription();
    outputDesc.putPartitionValue("ds", "foobar");
    outputDesc.setTableDesc(hiveTableDesc);

    HiveTableSchema schema = HiveTableSchemas.lookup(hiveServer.getClient(),
        null, hiveTableDesc);

    writeData(outputDesc, schema);

    HiveInputDescription inputDesc = new HiveInputDescription();
    inputDesc.setPartitionFilter("ds='foobar'");
    inputDesc.setTableDesc(hiveTableDesc);

    verifyData(inputDesc);
  }

  private void writeData(HiveOutputDescription outputDesc, HiveTableSchema schema)
      throws TException, IOException, InterruptedException
  {
    List<HiveWritableRecord> writeRecords = Lists.newArrayList();

    HiveWritableRecord r1 = HiveRecordFactory.newWritableRecord(schema);
    writeRecords.add(r1);
    r1.set(0, 1);
    r1.set(1, 1.1);

    HiveWritableRecord r2 = HiveRecordFactory.newWritableRecord(schema);
    writeRecords.add(r2);
    r2.set(0, 2);
    r2.set(1, 2.2);

    HiveOutput.writeTable(outputDesc, writeRecords);
  }

  private void verifyData(HiveInputDescription inputDesc)
      throws IOException, InterruptedException
  {
    Iterator<HiveReadableRecord> readRecords = HiveInput.readTable(inputDesc).iterator();

    assertTrue(readRecords.hasNext());

    HiveReadableRecord record = readRecords.next();
    assertEquals(Integer.class, record.get(0).getClass());
    assertEquals(Double.class, record.get(1).getClass());
    assertEquals(1, record.getInt(0));
    assertEquals(1.1, record.getDouble(1));

    assertTrue(readRecords.hasNext());
    record = readRecords.next();
    assertEquals(2, record.getInt(0));
    assertEquals(2.2, record.getDouble(1));

    assertFalse(readRecords.hasNext());
  }
}
