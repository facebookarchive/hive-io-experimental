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
package com.facebook.hiveio.tailer;

import com.facebook.hiveio.record.HiveReadableRecord;

/**
 * Printing records interface
 */
interface RecordPrinter {
  /**
   * Print a record
   *
   * @param record Hive record
   * @param numColumns number of columns
   * @param context Context
   * @param args command line args
   */
  void printRecord(HiveReadableRecord record, int numColumns, Context context,
      TailerArgs args);

  /**
   * Default printer
   */
  static class Default implements RecordPrinter {
    @Override public void printRecord(HiveReadableRecord record, int numColumns,
        Context context, TailerArgs args) {
      addRecordToStringBuilder(record, numColumns, context, args);
      context.perThread.get().flushBuffer();
    }

    /**
     * Add a record to the StringBuilder
     *
     * @param record Hive record
     * @param numColumns number of columns
     * @param context Context
     * @param args command line args
     */
    public static void addRecordToStringBuilder(HiveReadableRecord record,
        int numColumns, Context context, TailerArgs args) {
      StringBuilder sb = context.perThread.get().stringBuilder;
      sb.append(String.valueOf(record.get(0)));
      for (int index = 1; index < numColumns; ++index) {
        sb.append(args.separator);
        sb.append(String.valueOf(record.get(index)));
      }
    }
  }

  /**
   * Buffered printer
   */
  static class Buffered implements RecordPrinter {
    @Override public void printRecord(HiveReadableRecord record, int numColumns,
        Context context, TailerArgs args) {
      Default.addRecordToStringBuilder(record, numColumns, context, args);

      ThreadContext perThread = context.perThread.get();
      ++perThread.recordsInBuffer;
      if (perThread.recordsInBuffer >= args.recordBufferFlush) {
        perThread.flushBuffer();
      }
    }
  }
}
