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

package com.facebook.hiveio.input;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.hiveio.record.HiveReadableRecord;

/**
 * Observer for input operations
 */
public interface HiveApiInputObserver {
  /**
   * Start of reading
   * */
  void beginReadRow();

  /**
   * End of reading
   * @param key Key read
   * @param value Value read
   */
  void endReadRow(WritableComparable key, Writable value);

  /**
   * Failed to read row
   */
  void hiveReadRowFailed();

  /**
   * Begin parsing row
   */
  void beginParse();

  /**
   * Finished parsing row
   * @param record HiveRecord parsed
   */
  void endParse(HiveReadableRecord record);

  /**
   * An input observer that does nothing
   */
  class Empty implements HiveApiInputObserver {
    /** Singleton */
    private static final Empty INSTANCE = new Empty();

    /** Constructor */
    protected Empty() { }

    /**
     * Get singleton instance
     *
     * @return singleton instance
     */
    public static Empty get() {
      return INSTANCE;
    }

    @Override
    public void beginReadRow() { }

    @Override
    public void endReadRow(WritableComparable key, Writable value) { }

    @Override
    public void hiveReadRowFailed() { }

    @Override
    public void beginParse() { }

    @Override
    public void endParse(HiveReadableRecord record) { }
  }
}
