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

import org.apache.hadoop.hive.api.output.HiveApiOutputObserver;
import org.apache.hadoop.hive.api.HiveRecord;
import org.apache.hadoop.io.WritableComparable;

/**
 * Output observer that does nothing
 */
public class NoOpOutputObserver implements HiveApiOutputObserver {
  /** Singleton */
  private static final NoOpOutputObserver INSTANCE = new NoOpOutputObserver();

  /** Constructor */
  protected NoOpOutputObserver() { }

  /**
   * Get singleton
   * @return singelton instance
   */
  public static final NoOpOutputObserver get() {
    return INSTANCE;
  }

  @Override
  public void beginSerialize(WritableComparable<?> key, HiveRecord value) { }

  @Override
  public void serializeFailed(WritableComparable<?> key, HiveRecord value) { }

  @Override
  public void endSerialize(WritableComparable<?> key, HiveRecord value) { }

  @Override
  public void beginWrite(WritableComparable<?> key, HiveRecord value) { }

  @Override
  public void endWrite(WritableComparable<?> key, HiveRecord value) { }
}
