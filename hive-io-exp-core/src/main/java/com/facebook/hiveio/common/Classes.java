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

/**
 * Helpers for dealing with Classes in java.
 */
public class Classes {
  /** Don't construct, but allow inheritance */
  protected Classes() { }

  /**
   * Lookup Class by name. This is a helper to turn the exception thrown into
   * a RuntimeException to make things easier and code cleaner when it is known
   * fairly well that the lookup will not fail.
   *
   * @param name String name of class
   * @param <T> type of class
   * @return Class for name
   */
  public static <T> Class<? extends T> classForName(String name) {
    try {
      return (Class<? extends T>) Class.forName(name);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Cannot create class " + name, e);
    }
  }
}
