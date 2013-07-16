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
package com.facebook.hiveio.log;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Helper methods for Hive loggers
 */
public class HiveLogHelpers {
  /** Class logger */
  private static final Logger LOG = LoggerFactory.getLogger(HiveLogHelpers.class);

  /** Do not instantiate */
  private HiveLogHelpers() { }

  /**
   * Set the log level for Hive logs
   *
   * @param level Log level to set
   */
  public static void setHiveLogLevel(Level level) {
    List<Log> logs = Lists.newArrayList();
    logs.add(HiveMetaStore.LOG);
    addPrivateStaticLog(logs, Driver.class);
    addPrivateStaticLog(logs, ParseDriver.class);
    addPrivateStaticLog(logs, ObjectStore.class);

    for (Log log : logs) {
      setHiveLoggerLevel(log, level);
    }
  }

  /**
   * Set the log level for Hive logger
   *
   * @param log Hive logger
   * @param level Log level
   */
  public static void setHiveLoggerLevel(Log log, Level level) {
    if (log instanceof Log4JLogger) {
      Log4JLogger log4JLogger = (Log4JLogger) log;
      log4JLogger.getLogger().setLevel(level);
    } else {
      LOG.error("Don't know how to handle logger {} of type {}", log, log.getClass());
    }
  }

  /**
   * Add class logger to the list of logs
   *
   * @param logs List of logs
   * @param klass Class whose logger we want to add
   */
  private static void addPrivateStaticLog(List<Log> logs, Class<?> klass) {
    try {
      Field logField = klass.getDeclaredField("LOG");
      logField.setAccessible(true);
      Log driverLog = (Log) logField.get(null);
      logs.add(driverLog);
    } catch (IllegalAccessException e) {
      LOG.error("Could not get LOG from Hive ql.Driver", e);
    } catch (NoSuchFieldException e) {
      LOG.error("Could not get LOG from Hive ql.Driver", e);
    }
  }
}
