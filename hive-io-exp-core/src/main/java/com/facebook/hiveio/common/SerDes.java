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
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Helpers for dealing with Hive serializers and deserializers
 */
public class SerDes {
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(SerDes.class);

  /** Don't construct */
  protected SerDes() { }

  /**
   * Get Properties for configuring a SerDe
   *
   * @param fieldSchemas column information
   * @param serDeParams parameters to serde
   * @return Properties
   */
  public static Properties getSerDeProperties(
      List<FieldSchema> fieldSchemas, Map<String, String> serDeParams) {
    Properties props = new Properties();

    props.setProperty(serdeConstants.LIST_COLUMNS,
        MetaStoreUtils.getColumnNamesFromFieldSchema(fieldSchemas));
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        MetaStoreUtils.getColumnTypesFromFieldSchema(fieldSchemas));

    props.setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT, "\\N");
    props.setProperty(serdeConstants.SERIALIZATION_FORMAT, "1");

    props.putAll(serDeParams);

    return props;
  }

  /**
   * Initialize a Deserializer
   *
   * @param deserializer Deserializer to initialize
   * @param conf Configuration to use
   * @param fieldSchemas column information
   * @param params Deserializer parameters
   * @return configured Deserializer
   */
  public static Deserializer initDeserializer(
      Deserializer deserializer, Configuration conf,
      List<FieldSchema> fieldSchemas, Map<String, String> params) {
    Properties props = getSerDeProperties(fieldSchemas, params);
    try {
      deserializer.initialize(conf, props);
    } catch (SerDeException e) {
      throw new IllegalStateException("Initializing Deserializer " +
          deserializer, e);
    }
    return deserializer;
  }

  /**
   * Initialize a Serializer
   *
   * @param serializer Serializer to initialize
   * @param conf Configuration to use
   * @param fieldSchemas column information
   * @param params Serializer parameters
   * @return configured Serializer
   */
  public static Serializer initSerializer(
      Serializer serializer, Configuration conf,
      List<FieldSchema> fieldSchemas, Map<String, String> params) {
    Properties props = getSerDeProperties(fieldSchemas, params);
    try {
      serializer.initialize(conf, props);
    } catch (SerDeException e) {
      throw new IllegalStateException("Initializing Serializer " +
          serializer, e);
    }
    return serializer;
  }

  /**
   * Initialize a SerDe
   *
   * @param serDe SerDe to initialize
   * @param conf Configuration to use
   * @param fieldSchemas column information
   * @param params SerDe parameters
   * @return configured SerDe
   */
  public static SerDe initSerDe(
      SerDe serDe, Configuration conf,
      List<FieldSchema> fieldSchemas, Map<String, String> params) {
    Properties props = getSerDeProperties(fieldSchemas, params);
    try {
      serDe.initialize(conf, props);
    } catch (SerDeException e) {
      throw new IllegalStateException("Initializing Serializer " +
          serDe, e);
    }
    return serDe;
  }

  /**
   * Get Class object for SerDe
   * @param serDeInfo SerDe information
   * @return Class that extends SerDe
   */
  public static Class<? extends SerDe> getSerDeClass(SerDeInfo serDeInfo) {
    return Classes.classForName(serDeInfo.getSerializationLib());
  }

  /**
   * Create SerDe from class
   *
   * @param serDeClass class of SerDe
   * @return SerDe
   */
  public static SerDe createSerDe(Class<? extends SerDe> serDeClass) {
    SerDe serDe = null;
    try {
      Constructor<? extends SerDe> constructor = serDeClass.getDeclaredConstructor();
      constructor.setAccessible(true);
      serDe = constructor.newInstance();
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      LOG.error("Could not instantiate SerDe {}", serDeClass, e);
    }
    return serDe;
  }

  /**
   * Create SerDe from information
   *
   * @param serDeInfo SerDeInfo
   * @return SerDe
   */
  public static SerDe createSerDe(SerDeInfo serDeInfo) {
    return createSerDe(getSerDeClass(serDeInfo));
  }
}
