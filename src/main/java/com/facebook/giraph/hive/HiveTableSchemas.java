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

package com.facebook.giraph.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.facebook.giraph.hive.impl.HiveApiTableSchema;
import com.facebook.giraph.hive.impl.common.Writables;

/**
 * Helpers for Hive schemas
 */
public class HiveTableSchemas {
  /** Key for table schema for a name */
  public static final String TABLE_KEY_PREFIX = "hive.api.schema.table.";
  /** Key for table schema for a profile */
  public static final String PROFILE_KEY_PREFIX = "hive.api.schema.profile.";

  /** Logger */
  private static final Logger LOG = Logger.getLogger(HiveTableSchemas.class);

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
   * Configure object with hive table schema
   * @param obj Object
   * @param schema Hive table schema
   */
  public static void configure(HiveTableSchemaAware obj, HiveTableSchema schema) {
    obj.setTableSchema(schema);
  }

  /**
   * Get schema for a table
   * @param conf Configuration
   * @param dbName Database name
   * @param tableName Table name
   * @return schema
   */
  public static HiveTableSchema getForTable(Configuration conf, String dbName,
                                            String tableName) {

    return getImpl(conf, tableNameKey(dbName, tableName));
  }

  /**
   * Get schema for a profile
   * @param conf Configuration
   * @param profile Profile ID
   * @return schema
   */
  public static HiveTableSchema getForProfile(Configuration conf,
                                              String profile) {
    return getImpl(conf, profileKey(profile));
  }

  /**
   * Get schema from Configuration using key
   * @param conf Configuration
   * @param key String key
   * @return schema
   */
  private static HiveTableSchema getImpl(Configuration conf, String key) {
    String value = conf.get(key);
    if (value == null) {
      throw new NullPointerException("No HiveTableSchema with key " +
          key + " found");
    }
    HiveTableSchema hiveTableSchema = new HiveApiTableSchema();
    Writables.readFieldsFromEncodedStr(value, hiveTableSchema);
    return hiveTableSchema;
  }

  /**
   * Put schema for table
   * @param conf Configuration
   * @param dbName Database name
   * @param tableName Table name
   * @param hiveTableSchema schema
   */
  public static void putForName(Configuration conf, String dbName,
    String tableName, HiveTableSchema hiveTableSchema) {
    putImpl(conf, tableNameKey(dbName, tableName), hiveTableSchema);
  }

  /**
   * Put schema for profile
   * @param conf Configuraiton
   * @param profile Profile ID
   * @param hiveTableSchema schema
   */
  public static void putForProfile(Configuration conf, String profile,
                                   HiveTableSchema hiveTableSchema) {
    putImpl(conf, profileKey(profile), hiveTableSchema);
  }

  /**
   * Put schema for given key into Configuration
   * @param conf Configuration
   * @param key String key
   * @param schema Hive table schema
   */
  private static void putImpl(Configuration conf, String key,
                              HiveTableSchema schema) {
    conf.set(key, Writables.writeToEncodedStr(schema));
  }

  /**
   * Key for Configuration for a table
   *
   * @param dbName database name
   * @param tableName table name
   * @return key
   */
  private static String tableNameKey(String dbName, String tableName) {
    return TABLE_KEY_PREFIX + dbName + "." + tableName;
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
