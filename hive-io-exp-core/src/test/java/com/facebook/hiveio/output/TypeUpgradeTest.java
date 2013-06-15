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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.facebook.hiveio.common.HiveMetastores;
import com.facebook.hiveio.common.HiveTableName;
import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.record.HiveRecordFactory;
import com.facebook.hiveio.record.HiveWritableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.facebook.hiveio.schema.HiveTableSchemas;
import com.facebook.hiveio.testing.LocalHiveServer;
import com.google.common.collect.Lists;

import java.util.List;

import static org.testng.Assert.fail;

public class TypeUpgradeTest {
  private final LocalHiveServer hiveServer = new LocalHiveServer("hiveio-test");
  private final HiveTableName hiveTableName = new HiveTableName("default",
      TypeUpgradeTest.class.getSimpleName());

  @BeforeMethod
  public void beforeSuite() throws Exception {
    hiveServer.init();
    HiveMetastores.setTestClient(hiveServer.getClient());
  }

  @Test
  public void testDowngradeThrows() throws Exception {
    hiveServer.createTable("CREATE TABLE " + hiveTableName.getTableName() +
        " (t1 TINYINT, s1 SMALLINT, i1 INT, l1 BIGINT, " +
        "  f1 FLOAT, d1 DOUBLE) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");

    HiveOutputDescription outputDesc = new HiveOutputDescription();
    outputDesc.setHiveTableName(hiveTableName);

    HiveTableSchema schema = HiveTableSchemas.lookup(hiveServer.getClient(),
        null, hiveTableName);

    List<HiveWritableRecord> writeRecords = Lists.newArrayList();
    HiveWritableRecord r1 = HiveRecordFactory.newWritableRecord(schema);
    writeRecords.add(r1);

    r1.set(0, (byte) 4);
    checkSetThrows(r1, 0, (short) 4);
    checkSetThrows(r1, 0, 4);
    checkSetThrows(r1, 0, (long) 4);
    r1.set(0, null);

    r1.set(1, (byte) 4);
    r1.set(1, (short) 4);
    checkSetThrows(r1, 1, 4);
    checkSetThrows(r1, 1, (long) 4);
    r1.set(1, null);

    r1.set((byte) 2, 4);
    r1.set((short) 2, 4);
    r1.set(2, 4);
    checkSetThrows(r1, 2, (long) 4);
    r1.set(2, null);

    r1.set(4, 4.2f);
    checkSetThrows(r1, 4, 4.2d);
    r1.set(4, null);
  }

  private void checkSetThrows(HiveWritableRecord record, int index, Object data) {
    try {
      record.set(index, data);
    } catch (IllegalArgumentException e) {
      return;
    }
    fail();
  }

  @Test
  public void testUpgrade() throws Exception {
    hiveServer.createTable("CREATE TABLE " + hiveTableName.getTableName() +
        " (t1 TINYINT, s1 SMALLINT, i1 INT, l1 BIGINT, " +
        "  f1 FLOAT, d1 DOUBLE) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");

    HiveOutputDescription outputDesc = new HiveOutputDescription();
    outputDesc.setHiveTableName(hiveTableName);

    HiveInputDescription inputDesc = new HiveInputDescription();
    inputDesc.setHiveTableName(hiveTableName);

    HiveTableSchema schema = HiveTableSchemas.lookup(hiveServer.getClient(),
        null, hiveTableName);

    List<HiveWritableRecord> writeRecords = Lists.newArrayList();
    HiveWritableRecord r1 = HiveRecordFactory.newWritableRecord(schema);
    writeRecords.add(r1);

    r1.set(1, (byte) 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    r1.set(1, (short) 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    r1.set(1, null);
    HiveOutput.writeTable(outputDesc, writeRecords);

    r1.set(2, (byte) 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    r1.set(2, (short) 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    r1.set(2, 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    r1.set(2, null);
    HiveOutput.writeTable(outputDesc, writeRecords);

    r1.set(3, (byte) 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    r1.set(3, (short) 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    r1.set(3, 4);
    HiveOutput.writeTable(outputDesc, writeRecords);
    r1.set(3, 4L);
    HiveOutput.writeTable(outputDesc, writeRecords);
    r1.set(3, null);
    HiveOutput.writeTable(outputDesc, writeRecords);

    r1.set(5, 4.2f);
    HiveOutput.writeTable(outputDesc, writeRecords);
    r1.set(5, 4.2d);
    HiveOutput.writeTable(outputDesc, writeRecords);
  }
}
