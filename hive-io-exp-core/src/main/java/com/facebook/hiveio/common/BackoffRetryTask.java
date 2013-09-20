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
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A helper class that executes a task defined by idempotentTask() that
 * the subclass must implement. The task must be idempotent. When the task fails
 * with a TException, the task is retried with exponential backoff. The number
 * of retries and the initial delay are specified by the configuration.
 * A successful task returns a result. In contrast, an IOException is thrown
 * upon too many unsuccessful retries.
 *
 * @param <T> Task result
 */
public abstract class BackoffRetryTask<T> {
  /**
   * Key for the number of tries in a configuration.
   */
  public static final String NUM_TRIES_CONF_KEY = "hive.io.numtries";
  /**
   * Default number of tries.
   */
  public static final int NUM_TRIES_DEFAULT = 5;
  /**
   * Key for the initial retry delay in a configuration. The value of delay
   * should be low enough so that we do not need to call progress() throughout
   * retries.
   */
  public static final String INITIAL_RETRY_DELAY_MSEC_CONF_KEY =
      "hive.io.initialretrydelaymsec";
  /**
   * Default retry delay.
   */
  public static final int RETRY_DELAY_MSEC_DEFAULT = 10000;
  /**
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      BackoffRetryTask.class);
  /**
   * Number of task tries.
   */
  private final int numTries;
  /**
   * Initial delay to retry.
   */
  private final long initialRetryDelayMsec;

  /**
   * Constructs a task parametrized by the configuration.
   *
   * @param configuration Configuration
   */
  public BackoffRetryTask(Configuration configuration) {
    numTries = configuration.getInt(
        NUM_TRIES_CONF_KEY, NUM_TRIES_DEFAULT);
    initialRetryDelayMsec = configuration.getLong(
        INITIAL_RETRY_DELAY_MSEC_CONF_KEY, RETRY_DELAY_MSEC_DEFAULT);
  }

  /**
   * The task to be executed, and retried after TException.
   *
   * @return Task result
   * @throws TException
   */
  public abstract T idempotentTask() throws TException;

  /**
   * Executes the task, and retries after a TException.
   *
   * @return Task result
   * @throws IOException When the task has thrown TException too many times
   */
  public T run() throws IOException {
    boolean tryAgain = true;
    long delayMsec = initialRetryDelayMsec;
    for (int triesLeft = numTries; tryAgain && triesLeft > 0; --triesLeft) {
      try {
        return idempotentTask();
      } catch (TException e) {
        if (triesLeft == 1) {
          throw new IOException(e);
        } else {
          LOG.info("Failed, but will retry " + e);
          tryAgain = true;
          long randomDelayMsec = (long) (Math.random() * delayMsec);
          try {
            Thread.sleep(randomDelayMsec);
          } catch (InterruptedException interrupted) {
            tryAgain = false;
          }
          // Exponential backoff.
          delayMsec *= 2;
        }
      }
    }
    throw new IllegalStateException("We should never get here");
  }
}
