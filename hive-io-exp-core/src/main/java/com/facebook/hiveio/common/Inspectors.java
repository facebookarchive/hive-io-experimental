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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Helpers for creating ObjectInspectors.
 *
 * TODO: Most of this probably exists somewhere in Hive already, but I
 * couldn't find where?
 */
public class Inspectors {
  /** Don't construct, allow inheritance */
  protected Inspectors() { }

  /**
   * Create a StructObjectInspector for a list of fields
   *
   * @param fieldSchemas list of fields
   * @return StructObjectInspector
   */
  public static StructObjectInspector createFor(
      List<FieldSchema> fieldSchemas) {
    List<String> names = Lists.newArrayList();
    List<ObjectInspector> inspectors = Lists.newArrayList();
    for (FieldSchema fieldSchema : fieldSchemas) {
      names.add(fieldSchema.getName());
      inspectors.add(createFromFieldSchema(fieldSchema));
    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(names,
                                                                   inspectors);
  }

  /**
   * Create ObjectInspector for a column
   *
   * @param fieldSchema column info to use
   * @return ObjectInspector for column
   */
  private static ObjectInspector createFromFieldSchema(
      FieldSchema fieldSchema) {
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(
        fieldSchema.getType());
    return TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
  }
}
