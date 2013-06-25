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
import static org.testng.Assert.fail;

public class TypeUpgradeTest extends HiveIOTestBase {
  private static final double DELTA = 0.00001;

  private final LocalHiveServer hiveServer = new LocalHiveServer("hiveio-test");
  private final HiveTableDesc hiveTableDesc = new HiveTableDesc("default",
      TypeUpgradeTest.class.getSimpleName());

  @BeforeMethod
  public void beforeSuite() throws Exception {
    hiveServer.init();
    HiveMetastores.setTestClient(hiveServer.getClient());
  }

  @Test
  public void testDowngradeThrows() throws Exception {
    createTestTable();

    HiveOutputDescription outputDesc = new HiveOutputDescription();
    outputDesc.setTableDesc(hiveTableDesc);

    HiveInputDescription inputDesc = new HiveInputDescription();
    inputDesc.setTableDesc(hiveTableDesc);

    HiveTableSchema schema = HiveTableSchemas.lookup(hiveServer.getClient(),
        null, hiveTableDesc);

    List<HiveWritableRecord> writeRecords = Lists.newArrayList();
    HiveWritableRecord writeRecord = HiveRecordFactory.newWritableRecord(schema);
    writeRecords.add(writeRecord);

    HiveReadableRecord readRecord;

    writeRecord.set(0, (byte) 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getByte(0));

    checkSetThrows(writeRecord, 0, (short) 4);
    checkSetThrows(writeRecord, 0, 4);
    checkSetThrows(writeRecord, 0, (long) 4);
    checkSetThrows(writeRecord, 0, 4.2f);
    checkSetThrows(writeRecord, 0, 4.2d);

    recreateTable();
    writeRecord.set(0, null);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertTrue(readRecord.isNull(0));

    recreateTable();
    writeRecord.set(1, (byte) 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getShort(1));

    recreateTable();
    writeRecord.set(1, (short) 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getShort(1));

    checkSetThrows(writeRecord, 1, 4);
    checkSetThrows(writeRecord, 1, (long) 4);
    checkSetThrows(writeRecord, 1, 4.2f);
    checkSetThrows(writeRecord, 1, 4.2d);

    recreateTable();
    writeRecord.set(1, null);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertTrue(readRecord.isNull(1));

    recreateTable();
    writeRecord.set((byte) 2, 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getInt(2));

    recreateTable();
    writeRecord.set((short) 2, 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getInt(2));

    recreateTable();
    writeRecord.set(2, 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getInt(2));

    checkSetThrows(writeRecord, 2, (long) 4);
    checkSetThrows(writeRecord, 2, 4.2f);
    checkSetThrows(writeRecord, 2, 4.2d);

    recreateTable();
    writeRecord.set(2, null);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertTrue(readRecord.isNull(2));

    checkSetThrows(writeRecord, 3, 4.2f);
    checkSetThrows(writeRecord, 3, 4.2d);

    recreateTable();
    writeRecord.set(4, 4.2f);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4.2, readRecord.getFloat(4), DELTA);

    checkSetThrows(writeRecord, 4, 4.2d);

    recreateTable();
    writeRecord.set(4, null);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertTrue(readRecord.isNull(4));
  }

  private void recreateTable() throws TException {
    hiveServer.dropTable(hiveTableDesc.getTableName());
    createTestTable();
  }

  private void createTestTable() throws TException {
    hiveServer.createTable("CREATE TABLE " + hiveTableDesc.getTableName() +
        " (t1 TINYINT, s1 SMALLINT, i1 INT, l1 BIGINT, " +
        "  f1 FLOAT, d1 DOUBLE) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");
  }

  private static HiveReadableRecord readSingleRecord(HiveInputDescription inputDesc)
      throws IOException, InterruptedException {
    Iterator<HiveReadableRecord> iter = HiveInput.readTable(inputDesc).iterator();
    assertTrue(iter.hasNext());
    HiveReadableRecord record = iter.next();
    assertFalse(iter.hasNext());
    return record;
  }

  private static void checkSetThrows(HiveWritableRecord record, int index, Object data) {
    try {
      record.set(index, data);
    } catch (IllegalArgumentException e) {
      return;
    }
    fail();
  }

  @Test
  public void testUpgrade() throws Exception {
    createTestTable();

    HiveOutputDescription outputDesc = new HiveOutputDescription();
    outputDesc.setTableDesc(hiveTableDesc);

    HiveInputDescription inputDesc = new HiveInputDescription();
    inputDesc.setTableDesc(hiveTableDesc);

    HiveTableSchema schema = HiveTableSchemas.lookup(hiveServer.getClient(),
        null, hiveTableDesc);

    List<HiveWritableRecord> writeRecords = Lists.newArrayList();
    HiveWritableRecord r1 = HiveRecordFactory.newWritableRecord(schema);
    writeRecords.add(r1);

    HiveReadableRecord readRecord;

    r1.set(1, (byte) 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getShort(1));

    recreateTable();
    r1.set(1, (short) 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getShort(1));

    recreateTable();
    r1.set(2, (byte) 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getInt(2));

    recreateTable();
    r1.set(2, (short) 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getInt(2));

    recreateTable();
    r1.set(2, 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getInt(2));

    recreateTable();
    r1.set(3, (byte) 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getLong(3));

    recreateTable();
    r1.set(3, (short) 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getLong(3));

    recreateTable();
    r1.set(3, 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getLong(3));

    recreateTable();
    r1.set(3, 4L);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getLong(3));

    recreateTable();
    r1.set(4, (byte) 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getFloat(4), DELTA);

    recreateTable();
    r1.set(4, (short) 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getFloat(4), DELTA);

    recreateTable();
    r1.set(4, 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getFloat(4), DELTA);

    recreateTable();
    r1.set(4, 4L);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getFloat(4), DELTA);

    recreateTable();
    r1.set(4, 4.2f);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4.2, readRecord.getFloat(4), DELTA);

    recreateTable();
    r1.set(5, (byte) 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getDouble(5), DELTA);

    recreateTable();
    r1.set(5, (short) 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getDouble(5), DELTA);

    recreateTable();
    r1.set(5, 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getDouble(5), DELTA);

    recreateTable();
    r1.set(5, 4L);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4, readRecord.getDouble(5), DELTA);

    recreateTable();
    r1.set(5, 4.2f);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4.2, readRecord.getDouble(5), DELTA);

    recreateTable();
    r1.set(5, 4.2d);
    HiveOutput.writeTable(outputDesc, writeRecords);
    readRecord = readSingleRecord(inputDesc);
    assertEquals(4.2, readRecord.getDouble(5), DELTA);
  }
}
