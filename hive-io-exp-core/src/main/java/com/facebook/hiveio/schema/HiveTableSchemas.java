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

package com.facebook.hiveio.schema;

import com.facebook.hiveio.common.BackoffRetryTask;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.HiveMetastores;
import com.facebook.hiveio.common.HiveTableDesc;
import com.facebook.hiveio.common.HiveUtils;
import com.facebook.hiveio.common.Writables;
import com.google.common.base.Function;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

/**
 * Helpers for Hive schemas
 */
public class HiveTableSchemas {
  /** Key for table schema for a profile */
  public static final String PROFILE_KEY_PREFIX = "hive.io.schema.profile.";

  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(HiveTableSchemas.class);

  /** Don't construct */
  protected HiveTableSchemas() { }

  /**
   * Configure object with hive table schema, if it supports it
   *
   * @param obj Object
   * @param schema Hive table schema
   */
  public static void configure(Object obj, HiveTableSchema schema) {
    if (obj instanceof HiveTableSchemaAware) {
      ((HiveTableSchemaAware) obj).setTableSchema(schema);
    }
  }

  /**
   * Get function to lookup names in the table schema given
   *
   * @param tableSchema Hive table schema to use
   * @return Function that does lookup
   */
  public static Function<String, Integer>
  schemaLookupFunc(final HiveTableSchema tableSchema) {
    return new Function<String, Integer>() {
      @Override
      public Integer apply(String input) {
        return tableSchema.positionOf(input);
      }
    };
  }

  /**
   * Get table schema from Configuration, or if it is not present get it from metastore and set
   * it in Configuration
   *
   * @param conf Configuration
   * @param profile Profile id
   * @param tableName Name of the table
   * @return HiveTableSchema
   */
  public static HiveTableSchema initTableSchema(Configuration conf, String profile,
      final HiveTableDesc tableName) throws IOException {
    checkNotNull(conf, "conf is null");
    checkNotNull(profile, "profile is null");
    checkNotNull(tableName, "tableName is null");
    HiveTableSchema hiveTableSchema = getFromConf(conf, profile);
    if (hiveTableSchema == null) {
      hiveTableSchema = lookup(conf, tableName);
      putToConf(conf, profile, hiveTableSchema);
    }
    return hiveTableSchema;
  }

  /**
   * Get schema for a profile from Configuration
   *
   * @param conf Configuration
   * @param profile Profile ID
   * @return Schema, or null if it's not present in the Configuration
   */
  public static HiveTableSchema getFromConf(Configuration conf, String profile)
  {
    String key = profileKey(profile);
    String value = conf.get(key);
    if (value == null) {
      return null;
    }
    HiveTableSchema hiveTableSchema = new HiveTableSchemaImpl();
    Writables.readFieldsFromEncodedStr(value, hiveTableSchema);
    return hiveTableSchema;
  }

  /**
   * Put schema for a profile to Configuration
   *
   * @param conf Configuration
   * @param profile Profile ID
   * @param hiveTableSchema schema
   */
  public static void putToConf(Configuration conf, String profile,
                               HiveTableSchema hiveTableSchema) {
    conf.set(profileKey(profile), Writables.writeToEncodedStr(hiveTableSchema));
  }

  /**
   * Lookup schema from Hive metastore
   *
   * @param conf Configuration
   * @param tableName Hive table name
   * @return Hive table schema
   * @throws IOException When there are metastore issues
   */
  public static HiveTableSchema lookup(
      Configuration conf,
      final HiveTableDesc tableName) throws IOException {
    final HiveConf hiveConf =
        HiveUtils.newHiveConf(conf, HiveTableSchemas.class);
    BackoffRetryTask<Table> backoffRetryTask =
        new BackoffRetryTask<Table>(conf) {
          @Override
          public Table idempotentTask() throws TException {
            ThriftHiveMetastore.Iface client = HiveMetastores.create(hiveConf);
            return client.get_table(
                tableName.getDatabaseName(), tableName.getTableName());
          }
        };
    Table table = backoffRetryTask.run();
    return HiveTableSchemaImpl.fromTable(conf, table);
  }

  /**
   * Lookup schema from Hive metastore
   *
   *
   * @param client Metastore client
   * @param conf Configuration
   * @param tableName Hive table name
   * @return Hive table schema
   */
  public static HiveTableSchema lookup(ThriftHiveMetastore.Iface client,
      Configuration conf, HiveTableDesc tableName)
  {
    Table table;
    try {
      table = client.get_table(tableName.getDatabaseName(), tableName.getTableName());
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      throw new IllegalStateException(e);
    }
    return HiveTableSchemaImpl.fromTable(conf, table);
  }

  /**
   * Key for Configuration for a profile
   *
   * @param profile Profile ID
   * @return key
   */
  private static String profileKey(String profile) {
    return PROFILE_KEY_PREFIX + profile;
  }
}
