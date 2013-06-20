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
package com.facebook.hiveio.hadoop.shims.api;

/**
 * Shim for Configuration
 */
public interface ConfigurationShim {
  /**
   * Get value for key
   *
   * @param key config key
   * @return string value
   */
  String get(String key);

  /**
   * Get value for key or default value
   *
   * @param key config key
   * @param defaultValue default value
   * @return string value for key, or default value
   */
  String get(String key, String defaultValue);

  /**
   * Get boolean for key
   *
   * @param key config key
   * @param defaultValue default value
   * @return boolean value for key, or default value
   */
  boolean getBoolean(String key, boolean defaultValue);

  /**
   * Get float for key
   *
   * @param key config key
   * @param defaultValue default value
   * @return float value for key, or default value
   */
  float getFloat(String key, float defaultValue);

  /**
   * Get long for key
   *
   * @param key config key
   * @param defaultValue default value
   * @return long value for key, or default value
   */
  int getInt(String key, int defaultValue);

  /**
   * Get long for key
   *
   * @param key config key
   * @param defaultValue default value
   * @return long value for key, or default value
   */
  long getLong(String key, long defaultValue);

  /**
   * Set value for key
   *
   * @param key config key
   * @param value config value
   */
  void set(String key, String value);

  /**
   * Set value for key if it isn't set already
   *
   * @param key config key
   * @param value config value
   */
  void setIfUnset(String key, String value);

  /**
   * Set boolean for key if it isn't set already
   *
   * @param key config key
   * @param value config value
   */
  void setBooleanIfUnset(String key, boolean value);

  /**
   * Set boolean for key
   *
   * @param key config key
   * @param value value for key
   */
  void setBoolean(String key, boolean value);

  /**
   * Set float for key if it isn't set already
   *
   *     if (conf.get(getKey()) == null) {
         conf.setFloat(getKey(), value);
       }

   * @param key config key
   * @param value value for key
   */
  void setFloatIfUnset(String key, float value);

  /**
   * Set float for key
   *
   * @param key config key
   * @param value value for key
   */
  void setFloat(String key, float value);

  /**
   * Set int for key if it isn't set already
   *
   *     if (conf.get(getKey()) == null) {
         conf.setInt(getKey(), value);
       }

   * @param key config key
   * @param value value for key
   */
  void setIntIfUnset(String key, int value);

  /**
   * Set int for key
   *
   * @param key config key
   * @param value value for key
   */
  void setInt(String key, int value);

  /**
   * Set long for key
   *
   * @param key config key
   * @param value value for key
   */
  void setLong(String key, long value);
}
