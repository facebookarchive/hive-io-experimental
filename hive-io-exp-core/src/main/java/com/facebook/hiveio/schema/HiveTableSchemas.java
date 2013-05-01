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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.HiveMetastores;
import com.facebook.hiveio.common.HiveTableName;
import com.facebook.hiveio.common.HiveUtils;
import com.facebook.hiveio.common.Writables;
import com.google.common.base.Function;

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
   * Get schema for a profile
   * @param conf Configuration
   * @param profile Profile ID
   * @return schema
   */
  public static HiveTableSchema get(Configuration conf, String profile) {
    String key = profileKey(profile);
    String value = conf.get(key);
    if (value == null) {
      throw new NullPointerException("No HiveTableSchema with key " + key + " found");
    }
    HiveTableSchema hiveTableSchema = new HiveTableSchemaImpl();
    Writables.readFieldsFromEncodedStr(value, hiveTableSchema);
    return hiveTableSchema;
  }

  /**
   * Put schema for profile
   * @param conf Configuration
   * @param profile Profile ID
   * @param hiveTableSchema schema
   */
  public static void put(Configuration conf, String profile,
                         HiveTableSchema hiveTableSchema) {
    conf.set(profileKey(profile), Writables.writeToEncodedStr(hiveTableSchema));
  }

  /**
   * Put schema for a Hive table doing lookup in Hive metastore
   * @param conf Configuration
   * @param tableName Hive table name
   */
  public static void put(Configuration conf, String profile,
                         HiveTableName tableName)
  {
    put(conf, profile, lookup(conf, tableName));
  }

  /**
   * Lookup schema from Hive metastore
   * @param conf Configuration
   * @param tableName Hive table name
   * @return Hive table schema
   */
  public static HiveTableSchema lookup(Configuration conf, HiveTableName tableName)
  {
    HiveConf hiveConf = HiveUtils.newHiveConf(conf, HiveTableSchemas.class);
    ThriftHiveMetastore.Iface client;
    Table table;
    try {
      client = HiveMetastores.create(hiveConf);
      table = client.get_table(tableName.getDatabaseName(), tableName.getTableName());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return HiveTableSchemaImpl.fromTable(table);
  }

  /**
   * Lookup schema from Hive metastore
   * @param client Metastore client
   * @param tableName Hive table name
   * @return Hive table schema
   */
  public static HiveTableSchema lookup(ThriftHiveMetastore.Iface client, HiveTableName tableName)
  {
    Table table;
    try {
      table = client.get_table(tableName.getDatabaseName(), tableName.getTableName());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return HiveTableSchemaImpl.fromTable(table);
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
