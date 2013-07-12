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

import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.facebook.hiveio.common.HiveIOTestBase;
import com.facebook.hiveio.common.HiveMetastores;
import com.facebook.hiveio.common.HiveTableDesc;
import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.input.parser.Parsers;
import com.facebook.hiveio.input.parser.RecordParser;
import com.facebook.hiveio.input.parser.array.ArrayParser;
import com.facebook.hiveio.input.parser.array.ArrayRecord;
import com.facebook.hiveio.input.parser.hive.DefaultParser;
import com.facebook.hiveio.input.parser.hive.DefaultRecord;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.testing.LocalHiveServer;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class InputTest extends HiveIOTestBase {
  private static final double DELTA = 0.000001;
  private static final LocalHiveServer hiveServer = new LocalHiveServer("hiveio-test");
  private static final HiveTableDesc hiveTableDesc = new HiveTableDesc("test1");

  @BeforeMethod
  public void before() throws Exception {
    hiveServer.init();
    HiveMetastores.setTestClient(hiveServer.getClient());
  }

  @Test
  public void testInput() throws Exception
  {
    String rows[] = {
      "1\t1.1",
      "2\t2.2",
    };
    hiveServer.createTable("CREATE TABLE " + hiveTableDesc.getTableName() +
        " (i1 INT, d1 DOUBLE) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");
    hiveServer.loadData(hiveTableDesc.getTableName(), rows);

    HiveInputDescription inputDesc = new HiveInputDescription();
    inputDesc.getTableDesc().setTableName(hiveTableDesc.getTableName());

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
    inputDesc.getTableDesc().setTableName(tableName);
    inputDesc.setPartitionFilter(partition);

    verifyData(inputDesc);
  }

  private void checkGets(Class<? extends HiveReadableRecord> recordClass,
      Class<? extends RecordParser> parserClass) throws Exception {
    hiveServer.createTable("CREATE TABLE " + hiveTableDesc.getTableName() +
        " (t1 TINYINT, s1 SMALLINT, i1 INT, l1 BIGINT, " +
        "  b1 BOOLEAN, f1 FLOAT, d1 DOUBLE, str1 STRING, " +
        "  li1 ARRAY<BIGINT>, m1 MAP<STRING,FLOAT>) " +
        " ROW FORMAT DELIMITED " +
        " FIELDS TERMINATED BY '\t' " +
        " COLLECTION ITEMS TERMINATED BY ',' " +
        " MAP KEYS TERMINATED BY ':' ");
    String rows[] = {
      "1\t2\t3\t4\ttrue\t5.5\t6.6\tfoo\t1,2,3\tfoo:1.1,bar:2.2",
    };
    hiveServer.loadData(hiveTableDesc.getTableName(), rows);

    HiveInputDescription inputDesc = new HiveInputDescription();
    inputDesc.setTableDesc(hiveTableDesc);

    Configuration conf = new Configuration();
    Parsers.FORCE_PARSER.set(conf, parserClass);
    Iterator<HiveReadableRecord> records = HiveInput.readTable(inputDesc, conf).iterator();

    assertTrue(records.hasNext());

    HiveReadableRecord record = records.next();
    assertEquals(record.getClass(), recordClass);

    assertEquals(1, record.getByte(0));
    checkGetOthersThrows(record, 0, HiveType.BYTE);

    assertEquals(2, record.getShort(1));
    checkGetOthersThrows(record, 1, HiveType.SHORT);

    assertEquals(3, record.getInt(2));
    checkGetOthersThrows(record, 2, HiveType.INT);

    assertEquals(4, record.getLong(3));
    checkGetOthersThrows(record, 3, HiveType.LONG);

    assertEquals(true, record.getBoolean(4));
    checkGetOthersThrows(record, 4, HiveType.BOOLEAN);

    assertEquals(5.5, record.getFloat(5), DELTA);
    checkGetOthersThrows(record, 5, HiveType.FLOAT);

    assertEquals(6.6, record.getDouble(6), DELTA);
    checkGetOthersThrows(record, 6, HiveType.DOUBLE);

    assertEquals("foo", record.getString(7));
    checkGetOthersThrows(record, 7, HiveType.STRING);

    List<Long> list = record.getList(8);
    assertEquals(3, list.size());
    assertEquals(1, (long) list.get(0));
    assertEquals(2, (long) list.get(1));
    assertEquals(3, (long) list.get(2));

    Map<String, Float> map = record.getMap(9);
    assertEquals(2, map.size());
    assertEquals(1.1, map.get("foo"), DELTA);
    assertEquals(2.2, map.get("bar"), DELTA);

    assertFalse(records.hasNext());
  }

  @Test
  public void testGetDefault() throws Exception {
    checkGets(DefaultRecord.class, DefaultParser.class);
  }

  @Test
  public void testGetArray() throws Exception {
    checkGets(ArrayRecord.class, ArrayParser.class);
  }

  private void checkGetOthersThrows(HiveReadableRecord record, int index, HiveType hiveType) {
    for (HiveType otherType : EnumSet.complementOf(EnumSet.of(hiveType))) {
      checkGetThrows(record, index, otherType);
    }
  }

  private void checkGetThrows(HiveReadableRecord record, int index, HiveType hiveType) {
    try {
      record.get(index, hiveType);
    } catch (IllegalArgumentException e) {
      // expected this
      return;
    }
    fail();
  }

  private void verifyData(HiveInputDescription inputDesc)
      throws IOException, InterruptedException {
    Iterator<HiveReadableRecord> records = HiveInput.readTable(inputDesc).iterator();

    assertTrue(records.hasNext());

    HiveReadableRecord record = records.next();
    assertEquals(Integer.class, record.get(0).getClass());
    assertEquals(Double.class, record.get(1).getClass());
    assertEquals(1, record.getInt(0));
    assertEquals(1.1, record.getDouble(1));

    assertTrue(records.hasNext());
    record = records.next();
    assertEquals(2, record.getInt(0));
    assertEquals(2.2, record.getDouble(1));

    assertFalse(records.hasNext());
  }
}
