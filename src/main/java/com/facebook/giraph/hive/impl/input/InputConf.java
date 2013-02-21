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

package com.facebook.giraph.hive.impl.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.facebook.giraph.hive.impl.common.Writables;

/**
 * Hive Input configuration
 */
public class InputConf {
  /** Logger */
  public static final Logger LOG = Logger.getLogger(InputConf.class);

  /** Configuration key prefix */
  public static final String PREFIX_KEY = "hive.api.input";
  /** Configuration key for input info */
  public static final String INPUT_INFO_KEY = "input_info";
  /** Configuration key for number of splits */
  public static final String NUM_SPLITS_KEY = "num_splits";

  /** Hadoop Configuration */
  private final Configuration conf;
  /** Profile to use */
  private final String profileId;

  /**
   * Constructor
   *
   * @param conf Hadoop Configuration
   * @param profileId Profile ID
   */
  public InputConf(Configuration conf, String profileId) {
    this.conf = conf;
    this.profileId = profileId;
  }

  public String getProfileId() {
    return profileId;
  }

  private String getProfilePrefix() {
    return PREFIX_KEY + "." + profileId + ".";
  }

  public String getInputInfoKey() {
    return getProfilePrefix() + INPUT_INFO_KEY;
  }

  public String getNumSplitsKey() {
    return getProfilePrefix() + NUM_SPLITS_KEY;
  }

  /**
   * Read number of splits from Configuration
   *
   * @return int number of splits
   */
  public int readNumSplitsFromConf() {
    return conf.getInt(getNumSplitsKey(), 0);
  }

  /**
   * Read InputInfo from Configuration
   *
   * @return InputInfo
   */
  public InputInfo readInputInfoFromConf() {
    InputInfo ii = new InputInfo();
    String value = conf.get(getInputInfoKey());
    Writables.readFieldsFromEncodedStr(value, ii);
    return ii;
  }

  /**
   * Write number of splits to Configuration
   *
   * @param numSplits int number of splits
   */
  public void writeNumSplitsToConf(int numSplits) {
    conf.setInt(getNumSplitsKey(), numSplits);
  }

  /**
   * Write the profile ID to Configuration
   */
  public void writeProfileIdToConf() {
    conf.set(InputConf.PREFIX_KEY, profileId);
  }

  /**
   * Write input information to Configuration
   *
   * @param inputInfo InputInfo
   */
  public void writeInputInfoToConf(InputInfo inputInfo) {
    conf.set(getInputInfoKey(), Writables.writeToEncodedStr(inputInfo));
  }
}
