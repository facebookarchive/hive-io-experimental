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

package com.facebook.hiveio.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TException;

import com.facebook.hiveio.common.BackoffRetryTask;
import com.facebook.hiveio.common.Writables;
import com.facebook.hiveio.output.HiveOutputDescription;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

/**
 * Helpers for Hive tables
 */
public class HiveTables {
  /** Key for table schema for a profile */
  public static final String PROFILE_KEY_PREFIX = "hive.io.table.profile.";

  /** Don't construct */
  protected HiveTables() { }

  /**
   * Get table from Configuration, or if it is not present get it from metastore and set
   * it in Configuration
   *
   * @param conf Configuration
   * @param profile Profile id
   * @param outputDesc OutpputDesc of the table
   * @return Table
   */
  public static Table getTable(Configuration conf, String profile,
      HiveOutputDescription outputDesc) throws IOException {
    checkNotNull(conf, "conf is null");
    checkNotNull(profile, "profile is null");
    checkNotNull(outputDesc, "outputDesc is null");
    Table table = getFromConf(conf, profile);
    if (table == null) {
      table = lookup(conf, outputDesc);
      putToConf(conf, profile, table);
    }
    return table;
  }

  /**
   * Get table for a profile from Configuration
   *
   * @param conf Configuration
   * @param profile Profile ID
   * @return Table, or null if table is not set in the Configuration
   */
  public static Table getFromConf(Configuration conf, String profile)
  {
    checkNotNull(conf, "conf is null");
    checkNotNull(profile, "profile is null");
    String key = profileKey(profile);
    String value = conf.get(key);
    if (value == null) {
      return null;
    }
    return Writables.readObjectFromString(value);
  }

  /**
   * Put table for a profile
   *
   * @param conf Configuration
   * @param profile Profile ID
   * @param table Table
   */
  public static void putToConf(Configuration conf, String profile, Table table) {
    checkNotNull(conf, "conf is null");
    checkNotNull(profile, "profile is null");
    checkNotNull(table, "table is null");
    conf.set(profileKey(profile), Writables.writeObjectToString(table));
  }

  /**
   * Lookup table from Hive metastore
   *
   * @param conf Configuration
   * @param outputDesc Table outputDesc
   * @return Hive table
   * @throws IOException When there are metastore issues
   */
  private static Table lookup(
      final Configuration conf, final HiveOutputDescription outputDesc) throws IOException {
    checkNotNull(conf, "conf is null");
    checkNotNull(outputDesc, "outputDesc is null");
    BackoffRetryTask<Table> backoffRetryTask =
        new BackoffRetryTask<Table>(conf) {
          @Override
          public Table idempotentTask() throws TException {
            String dbName = outputDesc.getTableDesc().getDatabaseName();
            String tableName = outputDesc.getTableDesc().getTableName();
            ThriftHiveMetastore.Iface client = outputDesc.metastoreClient(conf);
            return client.get_table(dbName, tableName);
          }
        };
    return backoffRetryTask.run();
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
