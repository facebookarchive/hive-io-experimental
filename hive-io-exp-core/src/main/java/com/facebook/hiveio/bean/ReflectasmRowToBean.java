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
package com.facebook.hiveio.bean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.reflectasm.FieldAccess;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import java.lang.reflect.Field;
import java.util.List;

/**
 * {@link RowToBean} using reflectasm
 * @param <X> type
 */
public class ReflectasmRowToBean<X> implements RowToBean<X> {
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(ReflectasmRowToBean.class);

  /** Copiers for all the columns */
  private final List<FieldCopier> fieldCopiers = Lists.newArrayList();
  /** Accessor to internals */
  private final FieldAccess fieldAccess;

  /**
   * Constructor
   *
   * @param klass the class
   * @param tableSchema Hive schema
   */
  public ReflectasmRowToBean(Class<X> klass, HiveTableSchema tableSchema) {
    fieldAccess = FieldAccess.get(klass);
    for (Field field : klass.getDeclaredFields()) {
      field.setAccessible(true);
      String name = field.getName();
      ReflectasmFieldCopier fieldCopier = ReflectasmFieldCopier.fromType(field.getType());
      fieldCopier.setFieldAccess(fieldAccess);
      int hiveIndex = tableSchema.positionOf(name);
      if (hiveIndex == -1) {
        throw new IllegalArgumentException("Table " + tableSchema.getTableDesc() +
          " does not have column " + name);
      }
      fieldCopier.setFromHiveIndex(hiveIndex);
      fieldCopier.setToObjectIndex(fieldAccess.getIndex(name));
      fieldCopiers.add(fieldCopier);
    }
  }

  @Override public String toString() {
    return Objects.toStringHelper(this)
        .add("fieldCopiers", fieldCopiers)
        .toString();
  }

  @Override public void writeRow(HiveReadableRecord record, X result) {
    for (FieldCopier fieldCopier : fieldCopiers) {
      fieldCopier.setValue(record, result);
    }
  }
}

