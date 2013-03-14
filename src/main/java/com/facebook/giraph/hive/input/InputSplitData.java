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

package com.facebook.giraph.hive.input;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.facebook.giraph.hive.common.SerDes;
import com.facebook.giraph.hive.common.Writables;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

/**
 * Data which is common to both input splits and input partitions
 */
class InputSplitData implements Writable {
  /** Logger */
  public static final Logger LOG = Logger.getLogger(InputSplitData.class);

  /** CLass for Deserializer */
  private Class<? extends Deserializer> deserializerClass;
  /** Parameters to pass to Deserializer */
  private final Map<String, String> deserializerParams;

  /** Information about columns */
  private final List<FieldSchema> columnInfo;
  /** Partition data */
  private final Map<String, String> partitionValues;

  /**
   * Constructor
   */
  public InputSplitData() {
    deserializerParams = Maps.newHashMap();
    columnInfo = Lists.newArrayList();
    partitionValues = Maps.newHashMap();
  }

  /**
   * Constructor
   *
   * @param storageDescriptor StorageDescriptor
   */
  public InputSplitData(StorageDescriptor storageDescriptor) {
    columnInfo = storageDescriptor.getCols();
    SerDeInfo serDeInfo = storageDescriptor.getSerdeInfo();
    deserializerClass = SerDes.getSerDeClass(serDeInfo);
    deserializerParams = serDeInfo.getParameters();
    partitionValues = Maps.newHashMap();
  }

  /**
   * Parse partition values from Hive table/partition
   *
   * @param table Hive Table
   * @param partition Hive Partition
   */
  public void parsePartitionValues(Table table, Partition partition) {
    List<String> inputPartitionValues = partition.getValues();
    int i = 0;
    for (FieldSchema fieldSchema : table.getPartitionKeys()) {
      partitionValues.put(fieldSchema.getName().toLowerCase(),
          inputPartitionValues.get(i));
      ++i;
    }
  }

  /**
   * Create Deserializer
   *
   * @return Deserializer created
   */
  public Deserializer createDeserializer() {
    Preconditions.checkNotNull(deserializerClass);
    Deserializer deserializer = null;
    try {
      Constructor<? extends Deserializer> constructor =
          deserializerClass.getDeclaredConstructor();
      constructor.setAccessible(true);
      deserializer = constructor.newInstance();
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      LOG.fatal("Could not instantiate Deserializer " + deserializerClass, e);
    }
    return deserializer;
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

  public Map<String, String> getPartitionValues() {
    return partitionValues;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Writables.writeFieldSchemas(out, columnInfo);
    Writables.writeClassName(out,
        Preconditions.checkNotNull(deserializerClass));
    Writables.writeStrStrMap(out, deserializerParams);
    Writables.writeStrStrMap(out, partitionValues);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Writables.readFieldSchemas(in, columnInfo);
    deserializerClass = Writables.readClass(in);
    Writables.readStrStrMap(in, deserializerParams);
    Writables.readStrStrMap(in, partitionValues);
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
