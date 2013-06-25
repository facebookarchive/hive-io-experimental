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
package com.facebook.hiveio.testing;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.internal.Lists;

import java.lang.reflect.Field;
import java.util.List;

public class HiveLogHelpers {
  private static final Logger LOG = LoggerFactory.getLogger(HiveLogHelpers.class);

  private HiveLogHelpers() { }

  public static void setHiveLogLevel(Level level) {
    List<Log> logs = Lists.newArrayList();
    logs.add(HiveMetaStore.LOG);
    addPrivateStaticLog(logs, Driver.class);
    addPrivateStaticLog(logs, ParseDriver.class);

    for (Log log : logs) {
      setHiveLoggerLevel(log, level);
    }
  }

  public static void setHiveLoggerLevel(Log log, Level level) {
    if (log instanceof Log4JLogger) {
      Log4JLogger log4JLogger = (Log4JLogger) log;
      log4JLogger.getLogger().setLevel(level);
    } else {
      LOG.error("Don't know how to handle logger {} of type {}", log, log.getClass());
    }
  }

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
