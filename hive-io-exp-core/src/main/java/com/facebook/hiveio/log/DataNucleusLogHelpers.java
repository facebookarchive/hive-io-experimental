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

import org.apache.log4j.Level;
import org.datanucleus.util.Log4JLogger;
import org.datanucleus.util.NucleusLogger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

/**
 * Helper methods for DataNucleus loggers
 */
public class DataNucleusLogHelpers {
  /** Class logger */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(DataNucleusLogHelpers.class);

  /** Do not instantiate */
  private DataNucleusLogHelpers() { }

  /**
   * Set the log level for DataNucleus logs
   *
   * @param level Log level to set
   */
  public static void setDataNucleusLogLevel(Level level) {
    Field log4jLoggerField;
    try {
      log4jLoggerField = Log4JLogger.class.getDeclaredField("logger");
    } catch (NoSuchFieldException e) {
      LOG.error("Could not get field logger from {}", Log4JLogger.class.getCanonicalName());
      return;
    }
    log4jLoggerField.setAccessible(true);

    NucleusLogger[] nucleusLoggers = {
      NucleusLogger.DATASTORE,
      NucleusLogger.DATASTORE_PERSIST,
      NucleusLogger.DATASTORE_RETRIEVE,
      NucleusLogger.DATASTORE_SCHEMA,
      NucleusLogger.DATASTORE_NATIVE,
      NucleusLogger.METADATA,
      NucleusLogger.PERSISTENCE,
    };
    for (NucleusLogger nucleusLogger : nucleusLoggers) {
      setDatanucleusLoggerLevel(nucleusLogger, log4jLoggerField, level);
    }

    LOG.info("Done setting nucleus logger levels");
  }

  /**
   * Set the log level for Nucleus logger
   *
   * @param nucleusLogger Nucleus logger
   * @param log4jLoggerField Field which contains the logger
   * @param level Log level
   */
  public static void setDatanucleusLoggerLevel(NucleusLogger nucleusLogger,
      Field log4jLoggerField, Level level) {
    if (nucleusLogger instanceof Log4JLogger) {
      org.apache.log4j.Logger logger;
      try {
        logger = (org.apache.log4j.Logger) log4jLoggerField.get(nucleusLogger);
      } catch (IllegalAccessException e) {
        LOG.error("Could not access field logger from Log4jLogger");
        return;
      }
      logger.setLevel(level);
    } else {
      LOG.error("Don't know how to handle logger {} of type {}", nucleusLogger,
          nucleusLogger.getClass());
    }
  }
}
