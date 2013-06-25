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

import com.facebook.hiveio.common.HiveIOTestBase;
import com.facebook.hiveio.common.HiveMetastores;
import com.facebook.hiveio.testing.LocalHiveServer;
import com.google.common.base.Objects;

import java.util.Iterator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class BeanInputTest extends HiveIOTestBase {
  private LocalHiveServer hiveServer = new LocalHiveServer("hiveio-test");

  @BeforeMethod
  public void before() throws Exception {
    hiveServer.init();
    HiveMetastores.setTestClient(hiveServer.getClient());
  }

  public static class Row {
    private boolean bo1;
    private byte by1;
    private short s1;
    private int i1;
    private long l1;
    private float f1;
    private double d1;

    @Override public String toString() {
      return Objects.toStringHelper(this)
          .add("bo1", bo1)
          .add("by1", by1)
          .add("s1", s1)
          .add("i1", i1)
          .add("l1", l1)
          .add("f1", f1)
          .add("d1", d1)
          .toString();
    }
  }

  @Test
  public void testBean() throws Exception
  {
    String tableName = "test1";
    String input[] = {
      "false\t1\t2\t3\t4\t1.1\t2.2",
      "true\t2\t3\t4\t5\t2.2\t3.3",
    };

    hiveServer.createTable("CREATE TABLE " + tableName +
        " (bo1 BOOLEAN, " +
        "  by1 TINYINT, " +
        "  s1 SMALLINT, " +
        "  i1 INT, " +
        "  l1 BIGINT, " +
        "  f1 FLOAT, " +
        "  d1 DOUBLE" +
        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");
    hiveServer.loadData(tableName, input);

    HiveInputDescription inputDesc = new HiveInputDescription();
    inputDesc.getTableDesc().setTableName(tableName);

    Iterator<Row> rows = HiveInput.readTable(inputDesc, Row.class).iterator();

    assertTrue(rows.hasNext());
    Row row = rows.next();
    assertEquals(row.bo1, false);
    assertEquals(row.by1, 1);
    assertEquals(row.s1, 2);
    assertEquals(row.i1, 3);
    assertEquals(row.l1, 4);
    assertEquals(row.f1, 1.1, 0.000001);
    assertEquals(row.d1, 2.2, 0.000001);

    assertTrue(rows.hasNext());
    row = rows.next();
    assertEquals(row.bo1, true);
    assertEquals(row.by1, 2);
    assertEquals(row.s1, 3);
    assertEquals(row.i1, 4);
    assertEquals(row.l1, 5);
    assertEquals(row.f1, 2.2, 0.000001);
    assertEquals(row.d1, 3.3, 0.000001);

    assertFalse(rows.hasNext());
  }
}
