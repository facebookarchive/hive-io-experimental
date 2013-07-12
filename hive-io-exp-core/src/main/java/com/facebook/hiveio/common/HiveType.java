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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

/**
 * A Hive column type
 */
public enum HiveType {
  /** boolean */
  BOOLEAN {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Boolean);
      return data;
    }
    @Override public Class<?> javaClass() {
      return Boolean.class;
    }
    @Override public boolean isCollection() {
      return false;
    }
    @Override public boolean isIntegerType() {
      return false;
    }
  },
  /** byte */
  BYTE {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Byte);
      return data;
    }
    @Override public Class<?> javaClass() {
      return Byte.class;
    }
    @Override public boolean isCollection() {
      return false;
    }
    @Override public boolean isIntegerType() {
      return true;
    }
  },
  /** short */
  SHORT {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Short ||
        data instanceof Byte);
      return ((Number) data).shortValue();
    }
    @Override public Class<?> javaClass() {
      return Short.class;
    }
    @Override public boolean isCollection() {
      return false;
    }
    @Override public boolean isIntegerType() {
      return true;
    }
  },
  /** int */
  INT {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Integer ||
        data instanceof Short || data instanceof Byte);
      return ((Number) data).intValue();
    }
    @Override public Class<?> javaClass() {
      return Integer.class;
    }
    @Override public boolean isCollection() {
      return false;
    }
    @Override public boolean isIntegerType() {
      return true;
    }
  },
  /** long */
  LONG {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Long ||
        data instanceof Integer || data instanceof Short ||
        data instanceof Byte);
      return ((Number) data).longValue();
    }
    @Override public Class<?> javaClass() {
      return Long.class;
    }
    @Override public boolean isCollection() {
      return false;
    }
    @Override public boolean isIntegerType() {
      return true;
    }
  },
  /** float */
  FLOAT {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Float ||
          data instanceof Long || data instanceof Integer ||
          data instanceof Short || data instanceof Byte);
      return ((Number) data).floatValue();
    }
    @Override public Class<?> javaClass() {
      return Float.class;
    }
    @Override public boolean isCollection() {
      return false;
    }
    @Override public boolean isIntegerType() {
      return false;
    }
  },
  /** double */
  DOUBLE {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Number);
      return ((Number) data).doubleValue();
    }
    @Override public Class<?> javaClass() {
      return Double.class;
    }
    @Override public boolean isCollection() {
      return false;
    }
    @Override public boolean isIntegerType() {
      return false;
    }
  },
  /** string */
  STRING {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof String);
      return data;
    }
    @Override public Class<?> javaClass() {
      return String.class;
    }
    @Override public boolean isCollection() {
      return false;
    }
    @Override public boolean isIntegerType() {
      return false;
    }
  },
  /** list */
  LIST {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof List);
      return data;
    }
    @Override public Class<?> javaClass() {
      return List.class;
    }
    @Override public boolean isCollection() {
      return true;
    }
    @Override public boolean isIntegerType() {
      return false;
    }
  },
  /** map */
  MAP {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Map);
      return data;
    }
    @Override public Class<?> javaClass() {
      return Map.class;
    }
    @Override public boolean isCollection() {
      return true;
    }
    @Override public boolean isIntegerType() {
      return false;
    }
  },
  /** struct */
  STRUCT {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Map);
      return data;
    }
    @Override public Class<?> javaClass() {
      return Map.class;
    }
    @Override public boolean isCollection() {
      return true;
    }
    @Override public boolean isIntegerType() {
      return false;
    }
  },
  /** union */
  UNION {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Map);
      return data;
    }
    @Override public Class<?> javaClass() {
      return Map.class;
    }
    @Override public boolean isCollection() {
      return true;
    }
    @Override public boolean isIntegerType() {
      return false;
    }
  };

  /**
   * Java Class representing this enum
   *
   * @return Class
   */
  public abstract Class<?> javaClass();

  /**
   * Check if data is valid for this column. Upgrade it if necessary (for example
   * do Integer => Long conversion).
   *
   * @param data value to set
   * @return upgraded value
   */
  public abstract Object checkAndUpgrade(Object data);

  /**
   * Check if type represents an integral value
   *
   * @return true if integral value, false otherwise
   */
  public abstract boolean isIntegerType();

  public boolean isFloatingPoint() {
    return this == FLOAT || this == DOUBLE;
  }

  public boolean isMapLike() {
    return this == MAP || this == STRUCT;
  }

  /**
   * Check if type represents a collection
   *
   * @return true if collection, false otherwise
   */
  public abstract boolean isCollection();

  public boolean isPrimitive() {
    return !isCollection();
  }

  /**
   * Create from object inspector
   *
   * @param objectInspector Hive ObjectInspector
   * @return HiveType
   */
  public static HiveType fromHiveObjectInspector(ObjectInspector objectInspector) {
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(objectInspector);
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        PrimitiveObjectInspector primitiveInspector = (PrimitiveObjectInspector) objectInspector;
        return fromHivePrimitiveCategory(primitiveInspector.getPrimitiveCategory());
      case LIST:
        return LIST;
      case MAP:
        return MAP;
      case STRUCT:
        return STRUCT;
      case UNION:
        return UNION;
      default:
        throw new IllegalArgumentException("Can't handle typeInfo " + typeInfo);
    }
  }

  /**
   * Create from a Hive primitive category
   *
   * @param primitiveCategory Hive primitive category
   * @return HiveType
   */
  private static HiveType fromHivePrimitiveCategory(PrimitiveCategory primitiveCategory) {
    switch (primitiveCategory) {
      case BOOLEAN:
        return BOOLEAN;
      case BYTE:
        return BYTE;
      case SHORT:
        return SHORT;
      case INT:
        return INT;
      case LONG:
        return LONG;
      case FLOAT:
        return FLOAT;
      case DOUBLE:
        return DOUBLE;
      case STRING:
        return STRING;
      default:
        throw new IllegalArgumentException("Can't handle primitive category " +
            primitiveCategory);
    }
  }
}
