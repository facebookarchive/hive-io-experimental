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

import com.facebook.hiveio.record.HiveReadableRecord;
import com.google.common.base.Objects;

public abstract class FieldCopier {
  private static final Logger LOG = LoggerFactory.getLogger(FieldCopier.class);

  private int fromHiveIndex;

  protected int getFromHiveIndex() {
    return fromHiveIndex;
  }

  void setFromHiveIndex(int fromHiveIndex) {
    this.fromHiveIndex = fromHiveIndex;
  }

  @Override public String toString() {
    return Objects.toStringHelper(this)
        .add("fromHiveIndex", fromHiveIndex)
        .add("copierClass", getClass())
        .toString();
  }

  protected abstract void setValue(HiveReadableRecord fromRecord, Object toObject);
}
