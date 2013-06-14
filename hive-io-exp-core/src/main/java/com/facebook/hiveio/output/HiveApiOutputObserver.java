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

package com.facebook.hiveio.output;

import org.apache.hadoop.io.WritableComparable;

import com.facebook.hiveio.record.HiveWritableRecord;

/**
 * Observer for output operations
 */
public interface HiveApiOutputObserver {
  /**
   * Begin serializing key/value
   *
   * @param key Key
   * @param value Value
   */
  void beginSerialize(WritableComparable<?> key, HiveWritableRecord value);

  /**
   * Failed to serialize key/value
   *
   * @param key Key
   * @param value Value
   */
  void serializeFailed(WritableComparable<?> key, HiveWritableRecord value);

  /**
   * Finished serializing key/value
   *
   * @param key Key
   * @param value Value
   */
  void endSerialize(WritableComparable<?> key, HiveWritableRecord value);

  /**
   * Begin writing key/value
   *
   * @param key Key
   * @param value Value
   */
  void beginWrite(WritableComparable<?> key, HiveWritableRecord value);

  /**
   * Finished writing key/value
   *
   * @param key Key
   * @param value Value
   */
  void endWrite(WritableComparable<?> key, HiveWritableRecord value);

  /**
   * Output observer that does nothing
   */
  class Empty implements HiveApiOutputObserver {
    /** Singleton */
    private static final Empty INSTANCE = new Empty();

    /** Constructor */
    protected Empty() { }

    /**
     * Get singleton
     * @return singelton instance
     */
    public static Empty get() {
      return INSTANCE;
    }

    @Override
    public void beginSerialize(WritableComparable<?> key, HiveWritableRecord value) { }

    @Override
    public void serializeFailed(WritableComparable<?> key, HiveWritableRecord value) { }

    @Override
    public void endSerialize(WritableComparable<?> key, HiveWritableRecord value) { }

    @Override
    public void beginWrite(WritableComparable<?> key, HiveWritableRecord value) { }

    @Override
    public void endWrite(WritableComparable<?> key, HiveWritableRecord value) { }
  }
}
