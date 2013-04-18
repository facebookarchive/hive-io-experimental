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

import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

public enum HiveType {
  BOOLEAN(NativeType.BOOLEAN),
  BYTE(NativeType.LONG),
  SHORT(NativeType.LONG),
  INT(NativeType.LONG),
  LONG(NativeType.LONG),
  FLOAT(NativeType.DOUBLE),
  DOUBLE(NativeType.DOUBLE),
  STRING(NativeType.STRING),
  LIST(null),
  MAP(null),
  STRUCT(null),
  UNION(null);

  private final NativeType nativeType;

  HiveType(NativeType nativeType) {
    this.nativeType = nativeType;
  }

  public boolean isCollection() {
    return nativeType == null;
  }

  public boolean isPrimitive() {
    return nativeType != null;
  }

  public NativeType getNativeType() {
    return nativeType;
  }

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

  public static HiveType fromHivePrimitiveCategory(PrimitiveCategory primitiveCategory) {
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
