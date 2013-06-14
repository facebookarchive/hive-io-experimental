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

import com.facebook.hiveio.bean.ReflectasmRowToBean;
import com.facebook.hiveio.bean.RowToBean;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.rows.IdIdSimRow;
import com.facebook.hiveio.schema.HiveTableSchema;

/**
 * Row parser interface
 *
 * @param <Row> row class
 */
interface RowParser<Row> {
  /**
   * Parse a record
   *
   * @param record Hive record to parse
   */
  void parse(HiveReadableRecord record);

  /**
   * Default parser
   *
   * @param <X> row class
   */
  static class Default<X> implements RowParser<X> {
    @Override public void parse(HiveReadableRecord record) {
      for (int index = 0; index < record.numColumns(); ++index) {
        record.get(index);
      }
    }
  }

  /** long-long-double parser */
  static class LongLongDouble implements RowParser<IdIdSimRow> {
    @Override public void parse(HiveReadableRecord record) {
      record.getLong(0);
      record.getLong(1);
      record.getDouble(2);
    }
  }

  /**
   * Bean row parser
   *
   * @param <X> bean class
   */
  static class Bean<X> implements RowParser<X> {
    /** row */
    private final X row;
    /** bean mapper */
    private final RowToBean<X> rowMapper;

    /**
     * Constructor
     *
     * @param schema table schema
     * @param klass bean class
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public Bean(HiveTableSchema schema, Class<X> klass)
      throws IllegalAccessException, InstantiationException
    {
      row = klass.newInstance();
      rowMapper = new ReflectasmRowToBean<X>(klass, schema);
    }

    @Override public void parse(HiveReadableRecord record) {
      rowMapper.writeRow(record, row);
    }
  }
}
