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

package com.facebook.hiveio.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.input.HiveInputDescription;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Functions.forMap;
import static com.google.common.collect.Lists.transform;

/**
 * Utilities for dealing with Hive
 */
public class HiveUtils {
  /**
   * Function for getting the name from FieldSchema
   */
  public static final Function<FieldSchema, String> FIELD_SCHEMA_NAME_GETTER =
      new Function<FieldSchema, String>() {
        @Override
        public String apply(FieldSchema input) {
          return input == null ? null : input.getName();
        }
      };

  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(HiveUtils.class);

  /** Don't construct, allow inheritance */
  protected HiveUtils() { }

  /**
   * Create a new HiveConf
   *
   * @return HiveConf
   */
  public static HiveConf newHiveConf() {
    return alterHiveConf(new HiveConf());
  }

  /**
   * Create a new HiveConf with a class
   *
   * @param klass Class to use
   * @return HiveConf
   */
  public static HiveConf newHiveConf(Class<?> klass) {
    return alterHiveConf(new HiveConf(klass));
  }

  /**
   * Create a new HiveConf with a Configuration and a Class
   *
   * @param conf Configuration
   * @param klass Class
   * @return HiveConf
   */
  public static HiveConf newHiveConf(Configuration conf, Class<?> klass) {
    return alterHiveConf(new HiveConf(conf, klass));
  }

  /**
   * Alter the HiveConf before its returned to user
   *
   * @param hiveConf HiveConf
   * @return altered HiveConf
   */
  public static HiveConf alterHiveConf(HiveConf hiveConf) {
//    hiveConf.setVar(HiveConf.ConfVars.METASTORE_AUTO_START_MECHANISM_MODE, "None");
    return hiveConf;
  }

  /**
   * Get the statistics of a table
   *
   * @param client Metastore client
   * @param inputDesc description of Hive input table
   * @return HiveStats
   * @throws TException
   */
  public static HiveStats statsOf(ThriftHiveMetastore.Iface client,
      HiveInputDescription inputDesc) throws TException
  {
    HiveTableDesc tableDesc = inputDesc.getTableDesc();
    List<Partition> partitions = client.get_partitions_by_filter(
        tableDesc.getDatabaseName(), tableDesc.getTableName(),
        inputDesc.getPartitionFilter(), (short) -1);

    HiveStats hiveStats = new HiveStats();

    for (int i = 0; i < partitions.size(); ++i) {
      Partition partition = partitions.get(i);
      HiveStats partitionStats = HiveStats.fromParams(partition.getParameters());
      LOG.info("Adding HiveStats for partition {}: {}", partition.getValues(), partitionStats);
      hiveStats.add(partitionStats);
    }

    return hiveStats;
  }

  /**
   * Get Configuration value as list of URIs.
   *
   * @param conf Configuration to use
   * @param key String key to lookup
   * @return URI list from value
   */
  public static List<URI> getURIs(HiveConf conf, HiveConf.ConfVars key) {
    String[] parts = conf.getVar(key).split(",");
    List<URI> uris = Lists.newArrayList();
    for (int i = 0; i < parts.length; ++i) {
      URI uri;
      try {
        uri = new URI(parts[i]);
      } catch (URISyntaxException e) {
        LOG.error("URI syntax error", e);
        continue;
      }
      if (uri.getScheme() == null) {
        LOG.error("URI '{}' from key {} does not have a scheme", parts[i], key);
      } else {
        uris.add(uri);
      }
    }
    return uris;
  }

  /**
   * Get column types from table's SerDeInfo
   *
   * @param conf Configuration
   * @param storageDescriptor StorageDescriptor
   * @return array of HiveType
   * @throws SerDeException
   */
  public static HiveType[] columnTypes(Configuration conf, StorageDescriptor storageDescriptor)
  {
    SerDeInfo serDeInfo = storageDescriptor.getSerdeInfo();
    SerDe serDe = SerDes.createSerDe(serDeInfo);
    SerDes.initSerDe(serDe, conf, storageDescriptor.getCols(), serDeInfo.getParameters());
    StructObjectInspector inspector = null;
    try {
      inspector = (StructObjectInspector) serDe.getObjectInspector();
    } catch (SerDeException e) {
      LOG.error("Failed to get object inspector from table at {}",
          storageDescriptor.getLocation());
      throw new IllegalArgumentException("Failed to get object inspector from table at " +
          storageDescriptor.getLocation(), e);
    }
    List<? extends StructField> fields = inspector.getAllStructFieldRefs();
    HiveType[] types = new HiveType[fields.size()];
    for (int i = 0; i < fields.size(); ++i) {
      types[i] = HiveType.fromHiveObjectInspector(
          fields.get(i).getFieldObjectInspector());
    }
    return types;
  }

  /**
   * Get list of partition values in same order as partition keys passed in.
   * @param partitionKeys list of keys to grab
   * @param partitionValuesMap map of partition values
   * @return list of partition values
   */
  public static List<String> orderedPartitionValues(
      List<FieldSchema> partitionKeys, Map<String, String> partitionValuesMap) {
    List<String> partitionNames = transform(partitionKeys, FIELD_SCHEMA_NAME_GETTER);
    return transform(partitionNames, forMap(partitionValuesMap));
  }

  /**
   * Compute path to Hive partition
   * @param partitionKeys list of partition fields
   * @param partitionValuesMap partition values
   * @return path to partition for Hive table
   * @throws MetaException Hive meta issues
   */
  public static String computePartitionPath(List<FieldSchema> partitionKeys,
      Map<String, String> partitionValuesMap) throws MetaException {
    List<String> values = orderedPartitionValues(partitionKeys,
        partitionValuesMap);
    return Warehouse.makePartName(partitionKeys, values);
  }

  /**
   * Set ids of columns we're reading. Used by things like RCFile for skipping
   * other columns which we don't need.
   * @param conf Configuration to use
   * @param columnIds list of column ids
   */
  public static void setReadColumnIds(Configuration conf,
                                      List<Integer> columnIds) {
    if (columnIds.isEmpty()) {
      ColumnProjectionUtils.setFullyReadColumns(conf);
    } else {
      ColumnProjectionUtils.setReadColumnIDs(conf, columnIds);
    }
  }

  /**
   * Set ids of columns we're reading. Used by things like RCFile for skipping
   * other columns which we don't need.
   * @param conf Configuration to use
   * @param columnIds array of column ids
   */
  public static void setReadColumnIds(Configuration conf,
                                      int[] columnIds) {
    setReadColumnIds(conf, Ints.asList(columnIds));
  }

  /**
   * Set number of columns we're writing for RCFile's knowledge
   * @param conf Configuration to use
   * @param numColumns integer number of columns writing
   */
  public static void setRCileNumColumns(Configuration conf, int numColumns) {
    conf.set("hive.io.rcfile.column.number.conf",
        Integer.toOctalString(numColumns));
  }
}
