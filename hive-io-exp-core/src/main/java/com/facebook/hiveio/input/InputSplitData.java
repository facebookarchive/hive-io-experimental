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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.SerDes;
import com.facebook.hiveio.common.Writables;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Data which is common to both input splits and input partitions
 */
class InputSplitData implements Writable {
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(InputSplitData.class);

  /** CLass for Deserializer */
  private Class<? extends SerDe> deserializerClass;
  /** Parameters to pass to Deserializer */
  private final Map<String, String> deserializerParams;

  /** Information about columns */
  private final List<FieldSchema> columnInfo;
  /** Partition data */
  private final List<String> partitionValues;

  /** Constructor for reflection */
  public InputSplitData() {
    deserializerParams = Maps.newHashMap();
    columnInfo = Lists.newArrayList();
    partitionValues = Lists.newArrayList();
  }

  /**
   * Constructor from data
   * @param storageDescriptor StorageDescriptor
   * @param partitionValues list pf partition values
   */
  public InputSplitData(StorageDescriptor storageDescriptor,
                        List<String> partitionValues) {
    columnInfo = storageDescriptor.getCols();
    SerDeInfo serDeInfo = storageDescriptor.getSerdeInfo();
    deserializerClass = SerDes.getSerDeClass(serDeInfo);
    deserializerParams = serDeInfo.getParameters();
    this.partitionValues = partitionValues;
  }

  /**
   * Create Deserializer
   *
   * @return Deserializer created
   */
  public Deserializer createDeserializer() {
    return SerDes.createSerDe(deserializerClass);
  }

  /**
   * Check if we have a Deserializer class
   *
   * @return true if we have a Deserializer class
   */
  public boolean hasDeserializerClass() {
    return deserializerClass != null;
  }

  public Class<? extends Deserializer> getDeserializerClass() {
    return deserializerClass;
  }

  public Map<String, String> getDeserializerParams() {
    return deserializerParams;
  }

  public List<FieldSchema> getColumnInfo() {
    return columnInfo;
  }

  public List<String> getPartitionValues() {
    return partitionValues;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Writables.writeFieldSchemas(out, columnInfo);
    Writables.writeClassName(out,
        Preconditions.checkNotNull(deserializerClass));
    Writables.writeStrStrMap(out, deserializerParams);
    Writables.writeStringList(out, partitionValues);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Writables.readFieldSchemas(in, columnInfo);
    deserializerClass = Writables.readClass(in);
    Writables.readStrStrMap(in, deserializerParams);
    Writables.readStringList(in, partitionValues);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("columnInfo", columnInfo)
        .add("deserializerClass", deserializerClass)
        .add("deserializerParams", deserializerParams)
        .add("partitionValues", partitionValues)
        .toString();
  }
}
