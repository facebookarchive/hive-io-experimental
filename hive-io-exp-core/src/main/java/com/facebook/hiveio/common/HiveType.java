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
  BOOLEAN(NativeType.BOOLEAN) {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Boolean);
      return data;
    }
  },
  /** byte */
  BYTE(NativeType.LONG) {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Byte);
      return data;
    }
  },
  /** short */
  SHORT(NativeType.LONG) {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Short ||
        data instanceof Byte);
      return ((Number) data).shortValue();
    }
  },
  /** int */
  INT(NativeType.LONG) {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Integer ||
        data instanceof Short || data instanceof Byte);
      return ((Number) data).intValue();
    }
  },
  /** long */
  LONG(NativeType.LONG) {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Long ||
        data instanceof Integer || data instanceof Short ||
        data instanceof Byte);
      return ((Number) data).longValue();
    }
  },
  /** float */
  FLOAT(NativeType.DOUBLE) {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Float ||
          data instanceof Long || data instanceof Integer ||
          data instanceof Short || data instanceof Byte);
      return ((Number) data).floatValue();
    }
  },
  /** double */
  DOUBLE(NativeType.DOUBLE) {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Number);
      return ((Number) data).doubleValue();
    }
  },
  /** string */
  STRING(NativeType.STRING) {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof String);
      return data;
    }
  },
  /** list */
  LIST(null) {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof List);
      return data;
    }
  },
  /** map */
  MAP(null) {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Map);
      return data;
    }
  },
  /** struct */
  STRUCT(null) {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Map);
      return data;
    }
  },
  /** union */
  UNION(null) {
    @Override public Object checkAndUpgrade(Object data) {
      Preconditions.checkArgument(data instanceof Map);
      return data;
    }
  };

  /** Java native type representing this Hive column type */
  private final NativeType nativeType;

  /**
   * Constructor
   *
   * @param nativeType the native type
   */
  HiveType(NativeType nativeType) {
    this.nativeType = nativeType;
  }

  /**
   * Check if data is valid for this column. Upgrade it if necessary (for example
   * do Integer => Long conversion).
   *
   * @param data value to set
   * @return upgraded value
   */
  public abstract Object checkAndUpgrade(Object data);

  public boolean isCollection() {
    return nativeType == null;
  }

  public boolean isPrimitive() {
    return nativeType != null;
  }

  public NativeType getNativeType() {
    return nativeType;
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
