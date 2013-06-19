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

import com.facebook.hiveio.common.UnsafeHelper;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.List;

/**
 * {@link RowToBean} using {@link Unsafe}
 * @param <X> the type
 */
public class UnsafeRowToBean<X> implements RowToBean<X> {
  /** The Unsafe object */
  private static final Unsafe UNSAFE = UnsafeHelper.getUnsafe();
  /** Copiers for data */
  private final List<FieldCopier> fieldCopiers = Lists.newArrayList();

  /**
   * Constructor
   *
   * @param klass class we're copying to
   * @param schema Hive schema
   */
  public UnsafeRowToBean(Class<X> klass, HiveTableSchema schema) {
    for (Field field : klass.getDeclaredFields()) {
      String name = field.getName();
      field.setAccessible(true);
      UnsafeFieldCopier fieldCopier = UnsafeFieldCopier.fromType(field.getType());
      int hiveIndex = schema.positionOf(name);
      if (hiveIndex == -1) {
        throw new IllegalArgumentException("Table " + schema.getTableDesc() +
          " does not have column " + name);
      }
      fieldCopier.setFromHiveIndex(hiveIndex);
      fieldCopier.setToObjectOffset(UNSAFE.objectFieldOffset(field));
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
