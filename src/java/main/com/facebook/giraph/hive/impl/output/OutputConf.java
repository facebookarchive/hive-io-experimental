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

package org.apache.hadoop.hive.api.impl.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.api.output.HiveOutputDescription;
import org.apache.hadoop.hive.api.impl.common.Writables;

import com.google.common.base.Objects;

/**
 * Hive Output configuration
 */
public class OutputConf {
  /** Prefix for Configuration keys */
  public static final String PREFIX_KEY = "hive.api.output";
  /** Configuration key for table info */
  public static final String OUTPUT_TABLE_INFO_KEY = "table_info";
  /** Configuration key for description */
  public static final String OUTPUT_DESCRIPTION_KEY = "description";

  /** Hadoop Configuration */
  private final Configuration conf;
  /** Profile ID */
  private final String profileId;

  /**
   * Constructor
   *
   * @param conf Hadoop Configuration
   * @param profileId Profile ID
   */
  public OutputConf(Configuration conf, String profileId) {
    this.conf = conf;
    this.profileId = profileId;
  }

  public Configuration getConf() {
    return conf;
  }

  public String getProfileId() {
    return profileId;
  }

  public String getProfilePrefix() {
    return PREFIX_KEY + "." + profileId + ".";
  }

  public String getOutputTableInfoKey() {
    return getProfileId() + OUTPUT_TABLE_INFO_KEY;
  }

  public String getOutputDescriptionKey() {
    return getProfileId() + OUTPUT_DESCRIPTION_KEY;
  }

  /**
   * Read output table info from Configuration
   *
   * @return OutputInfo
   */
  public OutputInfo readOutputTableInfo() {
    OutputInfo oti = new OutputInfo();
    String value = conf.get(getOutputTableInfoKey());
    Writables.readFieldsFromEncodedStr(value,  oti);
    return oti;
  }

  /**
   * Write output table info to Configuration
   *
   * @param oti OutputInfo to write
   */
  public void writeOutputTableInfo(OutputInfo oti) {
    conf.set(getOutputTableInfoKey(), Writables.writeToEncodedStr(oti));
  }

  /**
   * Read user's output description from Configuration
   *
   * @return HiveOutputDescription
   */
  public HiveOutputDescription readOutputDescription() {
    HiveOutputDescription hod = new HiveOutputDescription();
    String value = conf.get(getOutputDescriptionKey());
    Writables.readFieldsFromEncodedStr(value, hod);
    return hod;
  }

  /**
   * Write user's output description to Configuration
   *
   * @param hod HiveOutputDescription
   */
  public void writeOutputDescription(HiveOutputDescription hod) {
    conf.set(getOutputDescriptionKey(), Writables.writeToEncodedStr(hod));
  }

  @Override public String toString() {
    return Objects.toStringHelper(this)
        .add("profileId", profileId)
        .toString();
  }
}
